"""Loopback-only physical fault input; never substitutes for P07/P12 real agents."""
import datetime,json,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit,parse_qs

MIB=1024*1024
class Agent:
 def __init__(self,port=56160):
  self.lock=threading.RLock();self.nodes={f's{i:02}':{'first':1,'last':16,'instance':7} for i in range(1,9)}
  self.mode='normal';self.target='s08';self.case='baseline';self.events=[];self.active=0;self.peak=0;self.barrier=None;self.arrived=set()
  owner=self
  class Handler(BaseHTTPRequestHandler):
   def do_GET(self):owner.respond(self)
   def log_message(self,*args):pass
  self.server=ThreadingHTTPServer(('127.0.0.1',port),Handler);self.server.daemon_threads=True
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
 def start(self):self.thread.start()
 def close(self):self.server.shutdown();self.server.server_close();self.thread.join(3)
 def change(self,mode,target='s08'):
  with self.lock:
   self.case=mode;self.mode=mode;self.target=target;self.active=0;self.peak=0;self.arrived=set()
   self.barrier=threading.Barrier(8,timeout=3) if mode=='concurrent' else None
   if mode=='gap':
    n=self.nodes[target];n.update(first=10**12,last=10**12+16)
   if mode=='maximum':
    n=self.nodes[target];first=n['last']+1;n.update(instance=n['instance']+1,first=first,last=first+9999)
 def snapshot(self):
  with self.lock:return {'mode':self.mode,'case':self.case,'peak_active':self.peak,'nodes':json.loads(json.dumps(self.nodes)),'events':list(self.events)}
 def streams(self,source,first,last,next_sequence,sequences,instance,pad=0):
  stamp=datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')
  obs=''.join(f'<Position dataItemId="position" sequence="{seq}" timestamp="{stamp}">{seq}</Position>' for seq in sequences)
  body=(f'<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7"><Header instanceId="{instance}" bufferSize="131072" firstSequence="{first}" lastSequence="{last}" nextSequence="{next_sequence}"/><Streams><DeviceStream name="{source}" uuid="acceptance-{source}"><ComponentStream component="Linear" componentId="x"><Samples>{obs}</Samples></ComponentStream></DeviceStream></Streams></MTConnectStreams>').encode()
  if pad:
   assert len(body)<=pad;body=body.replace(b'><Header',b'>'+b' '*(pad-len(body))+b'<Header',1);assert len(body)==pad
  return body
 def probe(self,source,instance,pad=0):
  body=(f'<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7"><Header instanceId="{instance}"/><Devices><Device id="{source}" name="{source}" uuid="acceptance-{source}"><Components><Linear id="x"><DataItems><DataItem id="position" name="position" category="SAMPLE" type="POSITION" units="MILLIMETER"/></DataItems></Linear></Components></Device></Devices></MTConnectDevices>').encode()
  if pad:body=body.replace(b'><Header',b'>'+b' '*(pad-len(body))+b'<Header',1);assert len(body)==pad
  return body
 def respond(self,h):
  parsed=urlsplit(h.path);parts=parsed.path.strip('/').split('/')
  if len(parts)!=2 or parts[0] not in self.nodes or parts[1] not in ['current','probe','sample']:h.send_error(404);return
  source,endpoint=parts;started=time.monotonic();error=None;sent=0;observations=0;barrier=None
  with self.lock:
   mode=self.mode if source==self.target or self.mode=='concurrent' else 'normal';case=self.case
   self.active+=1;self.peak=max(self.peak,self.active)
   if mode=='concurrent' and endpoint=='current' and source not in self.arrived:self.arrived.add(source);barrier=self.barrier
   node=self.nodes[source]
   if endpoint=='current' and mode not in ['maximum','slow','oversized-declared','oversized-stream']:
    node['last']+=4
   n=dict(node)
  try:
   if barrier:
    try:barrier.wait()
    except threading.BrokenBarrierError:pass
   if mode=='slow' and endpoint=='current':
    h.send_response(200);h.send_header('Content-Type','application/xml');h.end_headers()
    for _ in range(150):h.wfile.write(b' ');h.wfile.flush();sent+=1;time.sleep(0.2)
   elif mode=='oversized-declared' and endpoint=='current':
    h.send_response(200);h.send_header('Content-Length',str(8*MIB+1));h.end_headers()
   else:
    if mode=='oversized-stream' and endpoint=='current':body=b' '*(8*MIB+1)
    elif endpoint=='probe':body=self.probe(source,n['instance'],16*MIB if mode=='maximum' else 0)
    elif endpoint=='current':body=self.streams(source,n['first'],n['last'],n['last']+1,[n['last']],n['instance'],8*MIB if mode=='maximum' else 0)
    else:
     requested=int(parse_qs(parsed.query).get('from',[n['first']])[0]);count=int(parse_qs(parsed.query).get('count',[1000])[0])
     if mode=='oversized-observations':seqs=range(requested,requested+10001)
     elif mode=='maximum':seqs=range(requested,requested+10000)
     else:seqs=range(requested,min(n['last']+1,requested+count))
     observations=len(seqs);following=seqs[-1]+1 if observations else requested
     body=self.streams(source,n['first'],max(n['last'],following-1),following,seqs,n['instance']+(1 if mode=='event-storm' else 0),16*MIB if mode=='maximum' else 0)
    h.send_response(200);h.send_header('Content-Type','application/xml')
    if mode!='oversized-stream':h.send_header('Content-Length',str(len(body)))
    h.end_headers();h.wfile.write(body);h.wfile.flush();sent=len(body)
    if mode=='maximum' and endpoint=='sample':
     with self.lock:self.mode='normal'
  except (OSError,ConnectionError) as exc:error=type(exc).__name__
  finally:
   with self.lock:
    self.active=max(0,self.active-1)
    self.events.append({'case':case,'mode':mode,'source':source,'endpoint':endpoint,'started_monotonic':started,'elapsed_seconds':time.monotonic()-started,'sent_bytes':sent,'observations':observations,'error_type':error})
    self.events=self.events[-2000:]
