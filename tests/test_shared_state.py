"""Isolated sharing tests; no Kroger requests or real cart/token files."""
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pytest
from kroger_mcp import shared_state as state
from kroger_mcp.tools import cart_tools, shared

@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('KROGER_TOKEN_DIR', str(tmp_path))
    return tmp_path

def test_atomic_private_tokens(isolated):
    p=isolated/'token.json'
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: state.save_token({'fake':i},str(p)),range(50)))
    assert json.loads(p.read_text())['fake'] in range(50)
    assert p.stat().st_mode & 0o777 == 0o600
    assert not list(isolated.glob('.token.json.*'))

@pytest.mark.asyncio
async def test_concurrent_cart_cache_refresh(isolated):
    m=state.SharedStateMiddleware()
    active=maximum=refreshes=0
    cached=None
    async def mutate(_):
        nonlocal active,maximum,refreshes,cached
        active+=1;maximum=max(maximum,active)
        if cached is None:
            await asyncio.sleep(.002)
            cached={'fake':'valid'};refreshes+=1
            state.save_token(cached)
        cart_tools._add_item_to_local_cart('FAKE-ONLY',1,'PICKUP')
        shared.set_preferred_location_id('FAKE-STORE')
        await asyncio.sleep(.001)
        active-=1
    await asyncio.gather(*(m.on_call_tool(None,mutate) for _ in range(30)))
    assert maximum==1 and refreshes==1
    assert cart_tools._load_cart_data()['current_cart'][0]['quantity']==30
    assert shared.get_preferred_location_id()=='FAKE-STORE'

def test_order_transfer_crash_replay(isolated,monkeypatch):
    old={'current_cart':[{'product_id':'FAKE','quantity':1}]}
    cart_tools._save_cart_data(old)
    history=[{'items':old['current_cart']}]
    original=state.atomic_json
    def interrupted(path,value):
        if Path(path).name=='kroger_cart.json': raise OSError('simulated interruption')
        return original(path,value)
    monkeypatch.setattr(state,'atomic_json',interrupted)
    with pytest.raises(OSError): state.transfer_order({'current_cart':[]},history)
    assert (isolated/'.cart-transfer.json').exists()
    monkeypatch.setattr(state,'atomic_json',original)
    state.recover_transfer();state.recover_transfer()
    assert cart_tools._load_order_history()==history
    assert cart_tools._load_cart_data()['current_cart']==[]
    assert not (isolated/'.cart-transfer.json').exists()

@pytest.mark.asyncio
async def test_cancel_releases_lock(isolated):
    m=state.SharedStateMiddleware();entered=asyncio.Event()
    async def blocked(_): entered.set();await asyncio.sleep(60)
    task=asyncio.create_task(m.on_call_tool(None,blocked))
    await entered.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    async def ok(_): return 7
    assert await asyncio.wait_for(m.on_call_tool(None,ok),1)==7

@pytest.mark.asyncio
async def test_reload_rotated_cache(isolated):
    shared._authenticated_client=object()
    shared._client_credentials_client=object()
    async def verify(_):
        assert shared._authenticated_client is None
        assert shared._client_credentials_client is None
    await state.SharedStateMiddleware().on_call_tool(None,verify)

def test_finite_network_timeout():
    class FakeRequests:
        def request(self,*args,**kwargs): return kwargs
    bounded=state.BoundedRequests(FakeRequests())
    assert bounded.request('GET','https://fake.invalid')['timeout']==(5,20)
    assert bounded.request('GET','https://fake.invalid',timeout=1)['timeout']==1

@pytest.mark.asyncio
async def test_process_lock_and_rotated_token(isolated):
    import subprocess,sys
    code='''
import asyncio,json,time
from pathlib import Path
from kroger_mcp import shared_state as state
from kroger_api.token_storage import get_token_file_path
async def main():
    async def mutate(_):
        p=Path(get_token_file_path('fake-rotation.json'))
        value=json.loads(p.read_text()) if p.exists() else {'generation':0}
        await asyncio.sleep(.01)
        value['generation']+=1
        state.atomic_json(p,value)
    m=state.SharedStateMiddleware()
    for _ in range(10): await m.on_call_tool(None,mutate)
asyncio.run(main())
'''
    a=subprocess.Popen([sys.executable,'-c',code])
    b=subprocess.Popen([sys.executable,'-c',code])
    assert await asyncio.to_thread(a.wait,20)==0
    assert await asyncio.to_thread(b.wait,20)==0
    assert json.loads((isolated/'fake-rotation.json').read_text())['generation']==20

@pytest.mark.asyncio
async def test_oauth_attempts_are_session_scoped(monkeypatch):
    from kroger_mcp.tools import auth
    functions={}
    class FakeMcp:
        def tool(self):
            def record(fn): functions[fn.__name__]=fn;return fn
            return record
    class FakeAPI:
        authorization=None
        def __init__(self): self.authorization=self
        def get_authorization_url(self,**kwargs): return 'https://fake.invalid/'
    class Ctx:
        def __init__(self,id): self.session_id=id
        async def info(self,*args): pass
        async def error(self,*args): pass
    monkeypatch.setenv('KROGER_CLIENT_ID','fake-id')
    monkeypatch.setattr(auth,'KrogerAPI',FakeAPI)
    auth.register_auth_tools(FakeMcp())
    await functions['start_authentication'](Ctx('A'))
    first=auth._auth_attempts['A']
    await functions['start_authentication'](Ctx('B'))
    assert auth._auth_attempts['A']==first
    assert auth._auth_attempts['B']!=first
    wrong=await functions['complete_authentication']('https://fake.invalid/?code=fake&state=wrong',Ctx('A'))
    assert wrong['error'] is True
    auth._auth_attempts.clear()
