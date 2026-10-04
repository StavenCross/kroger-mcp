"""Shared-service serialization and crash-safe private JSON persistence."""
import asyncio
import fcntl
import json
import os
import tempfile
from pathlib import Path
from fastmcp.server.middleware import Middleware
from kroger_api.token_storage import get_token_file_path


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def save_token(token_info, token_file='.kroger_tokens.json'):
    atomic_json(get_token_file_path(token_file), token_info)


def recover_transfer():
    journal = Path(get_token_file_path('.cart-transfer.json'))
    if journal.exists():
        data = json.loads(journal.read_text())
        atomic_json(get_token_file_path('kroger_order_history.json'), data['history'])
        atomic_json(get_token_file_path('kroger_cart.json'), data['cart'])
        journal.unlink()
        fd = os.open(journal.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def transfer_order(cart, history):
    atomic_json(get_token_file_path('.cart-transfer.json'), {'cart': cart, 'history': history})
    recover_transfer()


class SharedStateMiddleware(Middleware):
    """Serialize whole tool calls, including awaits and token refresh.

    File lock also coordinates separately launched updated processes. Existing
    older stdio processes do not honor this lock and must not mutate concurrently.
    Cancellation releases locks; uncertain remote writes are never retried here.
    """
    def __init__(self):
        self.lock = asyncio.Lock()

    async def on_call_tool(self, context, call_next):
        async with self.lock:
            path = get_token_file_path('.shared-service.lock')
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        await asyncio.sleep(0.05)
                recover_transfer()
                # Other updated processes may have rotated OAuth tokens.
                from .tools.shared import invalidate_authenticated_client, invalidate_client_credentials_client
                invalidate_authenticated_client()
                invalidate_client_credentials_client()
                return await call_next(context)
            finally:
                os.close(fd)


class BoundedRequests:
    """Scope finite network timeouts to Kroger's dependency, without retrying."""
    def __init__(self, requests_module):
        self.module = requests_module

    def __getattr__(self, name):
        original = getattr(self.module, name)
        if name not in ('get', 'post', 'request'):
            return original
        def bounded(*args, **kwargs):
            kwargs.setdefault('timeout', (5, 20))
            return original(*args, **kwargs)
        return bounded
