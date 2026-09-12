"""Record exact neighboring test order from the retained failing native JUnit."""
import json,pathlib,xml.etree.ElementTree as ET,zipfile
dest=pathlib.Path(__file__).resolve().parent
path=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance\pr475-completed-raw-artifacts\10302576589.zip')
targets={'D14':'test_skip_serializes_with_concurrent_benchmark_completion','D15':'test_three_device_federation_keeps_ai_compute_and_storage_authority_separate'}
out={'candidate_sha':'5e6f184311019b9982e8544a18f3dc02c1b16e98','run':34707260030,'job':103589446004,'artifact':10302576589,'seed':1702,'contexts':{}}
with zipfile.ZipFile(path) as archive:
    for name in archive.namelist():
        if not name.endswith('.xml'):continue
        cases=ET.fromstring(archive.read(name)).findall('.//testcase')
        for i,c in enumerate(cases):
            for identifier,test in targets.items():
                if c.get('name')==test:
                    out['contexts'][identifier]={'index':i,'xml':name,'neighbors':[{'index':k,'class':cases[k].get('classname'),'test':cases[k].get('name'),'seconds':cases[k].get('time')} for k in range(max(0,i-8),min(len(cases),i+3))]}
(dest/'D14-D15-native-order-context.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps(out))
