"""Observe codes/exception locals in isolated tests; never bypass an assertion."""
import datetime,json,pathlib
import pytest

observations=[]

@pytest.fixture(autouse=True)
def observe_inspection_refusal(request,monkeypatch):
    if request.node.name!='test_three_device_federation_keeps_ai_compute_and_storage_authority_separate':
        yield
        return
    from catalog.flask_app import capability_inspection_routes as routes
    original=routes._safe_error_response
    def record(code,status):
        observations.append({'test':request.node.nodeid,'inspection_error_code':code,'http_status':status})
        return original(code,status)
    monkeypatch.setattr(routes,'_safe_error_response',record)
    yield

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item,call):
    outcome=yield
    report=outcome.get_result()
    if report.when!='call':return
    entry={'test':item.nodeid,'outcome':report.outcome,'duration':report.duration}
    if call.excinfo:
        entry['exception_type']=call.excinfo.typename
        for frame in call.excinfo.traceback:
            if frame.name=='test_skip_serializes_with_concurrent_benchmark_completion':
                local=frame.frame.f_locals
                entry['run_errors']=[type(e).__name__+': '+str(e) for e in local.get('run_errors',[])]
                thread=local.get('run_thread')
                entry['run_thread_alive']=thread.is_alive() if thread else None
    observations.append(entry)

def pytest_sessionfinish(session,exitstatus):
    path=pathlib.Path(__file__).resolve().parent/'D14-D15-focused-observer.json'
    path.write_text(json.dumps({'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':'5e6f184311019b9982e8544a18f3dc02c1b16e98','context':'NETTKING Ubuntu WSL, isolated development venv; distinct from original AQG runner','exitstatus':exitstatus,'observations':observations,'product_source_changed':False,'protected_recorder_data':'UNTOUCHED','qualification':False},indent=2)+'\n',encoding='utf-8')
