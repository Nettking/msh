"""Diagnostic only: no changed deadlines, assertions, responses or synchronization."""
import datetime
import functools
import json
import os
from pathlib import Path
import time

enabled = False
path = Path(os.environ['FCP_CATCHUP_TRACE'])
path.parent.mkdir(parents=True, exist_ok=True)

def emit(event, **fields):
    with path.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps({'utc':datetime.datetime.now(datetime.UTC).isoformat(),'mono':time.monotonic(),'event':event,**fields}, default=str)+'\n')

def pytest_runtest_logstart(nodeid, location):
    global enabled
    enabled = 'test_live_catchup_repairs_only_missing_batches' in nodeid
    emit('test-start', nodeid=nodeid)

def pytest_runtest_logreport(report):
    emit('test-report',nodeid=report.nodeid,phase=report.when,outcome=report.outcome,duration=report.duration,traceback=str(report.longrepr) if report.failed else None)

def pytest_collection_finish(session):
    emit('collected-order',nodeids=[item.nodeid for item in session.items])

def wrap(cls, name):
    original=getattr(cls,name)
    @functools.wraps(original)
    async def observed(self,*args,**kwargs):
        if not enabled:
            return await original(self,*args,**kwargs)
        start=time.monotonic()
        fields={'phase':cls.__name__+'.'+name}
        envelope=kwargs.get('envelope')
        if envelope is not None:
            fields.update(request_id=envelope.request_id,operation=str(envelope.operation),timeout=getattr(self,'request_timeout',None))
        if name=='_inspect':
            fields.update(provider_id=args[1],item_id=args[3])
        if name=='_handle_request':
            frame=json.loads(args[1].get('frame','{}'))
            fields.update(request_id=frame.get('request_id'),operation=frame.get('operation'))
        emit('enter',**fields)
        try:
            result=await original(self,*args,**kwargs)
        except BaseException as exc:
            emit('exception',**fields,duration=time.monotonic()-start,error_type=type(exc).__name__,error_code=getattr(exc,'code',None),reason=str(exc))
            raise
        else:
            emit('return',**fields,duration=time.monotonic()-start,ok=getattr(result,'ok',None),error_code=getattr(getattr(result,'error',None),'code',None))
            if name=='run_once':
                emit('catchup-result',status=result.status,code=result.code,attempted=result.attempted,delivered=result.delivered,reconciled=result.reconciled,items=[{'item':i.item_id,'status':i.status,'attempts':i.attempt_count,'error_code':i.last_error_code,'reason':i.last_error_reason} for i in result.record.items] if result.record else [])
            return result
    setattr(cls,name,observed)

def pytest_configure(config):
    from catalog.federation.live_catchup import LiveFormerPrimaryCatchupCoordinator
    from catalog.federation.relay_storage import RelayStorageEndpoint
    for name in ('run_once','_inspect','_deliver'):
        wrap(LiveFormerPrimaryCatchupCoordinator,name)
    for name in ('request','_handle_request'):
        wrap(RelayStorageEndpoint,name)
    emit('observer-configured',base_source='3a908fd1d9503057f3520d8f2dbbf62401a6e6d3',diagnostic_source=os.environ.get('GITHUB_SHA'))
