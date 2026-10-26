/* Handwritten browser-controller tests. No real network, credentials or data. */
'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const code = fs.readFileSync(path.join(__dirname,'../static/js/process-twin.js'),'utf8');
class Element {
  constructor() { this.value=''; this.hidden=false; this.dataset={csrf:'synthetic'}; this.children=[]; this.events={}; }
  addEventListener(name, callback) { this.events[name]=callback; }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.children=[...items]; this.textContent=''; this.value=items[0]?.value||''; }
  querySelectorAll() { return []; }
  focus() {}
}
const tick = () => new Promise(resolve=>setImmediate(resolve));
const result = (data,meta={}) => ({ok:true,status:200,json:async()=>({data,meta})});
async function controller(handler,twins=[]) {
  const elements=new Map();
  const element=id=>{ if(!elements.has(id)) elements.set(id,new Element()); return elements.get(id); };
  const calls=[];
  const fetch=async(url,options)=>{
    calls.push({url,options});
    if(url.endsWith('/inputs')) return result({items:[{input_id:'synthetic-input',binding_revision:'synthetic-revision',kind:'CANONICAL_DEVICE',can_prepare:true,display_name:'Synthetic'}]});
    if(url.endsWith('/twins')) return result({items:twins,total:twins.length,has_more:false});
    return handler(url,options);
  };
  vm.runInNewContext(code,{document:{getElementById:element,createElement:()=>new Element()},fetch,Option:class{constructor(label,value){this.textContent=label;this.value=value;}},crypto:{randomUUID:()=> '00000000-0000-0000-0000-000000000001'},console});
  await tick(); await tick();
  element('pt-input').value='synthetic-input'; element('pt-time-policy').value='SOURCE_UTC';
  const submit=async()=>{ element('pt-prepare').events.submit({preventDefault(){},submitter:new Element()}); await tick(); await tick(); };
  return {element,calls,submit};
}
test('lost response resolves original key without another POST',async()=>{
  const app=await controller(async(url,options)=>{
    if(options.method==='POST') throw new Error('synthetic response loss');
    if(url.includes('/commands/by-key')) return result({status:'SUCCEEDED',result_location:'/api/process-twins/v1/dataset-revisions/synthetic'});
    return result({dataset_revision_id:'synthetic'});
  });
  await app.submit();
  const writes=app.calls.filter(c=>c.options.method==='POST');
  const lookup=app.calls.find(c=>c.url.includes('/commands/by-key'));
  assert.equal(writes.length,1);
  assert.equal(lookup.options.headers['Idempotency-Key'],writes[0].options.headers['Idempotency-Key']);
  assert.match(app.element('pt-prepared').textContent,/klargjort/);
  assert.equal(app.element('pt-command-status').hidden,true);
});
test('pending command blocks changed request and retains original status lookup',async()=>{
  let running=true;
  const app=await controller(async(url,options)=>{
    if(options.method==='POST') throw new Error('synthetic response loss');
    if(url.includes('/commands/by-key')) return result({status:running?'RUNNING':'SUCCEEDED',result_location:'/api/process-twins/v1/dataset-revisions/synthetic'});
    return result({dataset_revision_id:'synthetic'});
  });
  await app.submit();
  app.element('pt-time-policy').value='UNRESOLVED';
  await app.submit();
  assert.match(app.element('pt-status').textContent,/tidligere bestillingen/);
  assert.equal(app.calls.filter(c=>c.options.method==='POST').length,1);
  running=false;
  app.element('pt-command-status').events.click(); await tick(); await tick();
  assert.match(app.element('pt-status').textContent,/Ingen ny bestilling/);
  assert.equal(app.element('pt-command-status').hidden,true);
});

const windowFor = id => ({start_at:`1970-01-0${id==='a'?1:2}T00:00:00.000000Z`,end_at:`1970-01-0${id==='a'?2:3}T00:00:00.000000Z`});
const manifest = (id,partial=false) => result({snapshot_id:id,window:windowFor(id),services:{history:{state:'AVAILABLE',reason_codes:[]}},summary:{covered_duration_us:1},available_views:['episodes']},{technical_state:partial?'PARTIAL':'AVAILABLE',technical_reason_codes:partial?['VIEW_INDEX_UNAVAILABLE']:[]});
const page = (id,more=true) => result({items:[{row_id:id,row_kind:'EPISODE',classification:'Synthetic '+id,original_start_at:windowFor('a').start_at,reason_codes:[],evidence_refs:[id+'-evidence']}],has_more:more,next_cursor:more?'cursor-'+id:null,total:more?2:1});
const settle = async() => {await tick();await tick();};
const click = async node => {node.events.click();await settle();};
async function historyController(handler) {
  const app=await controller(async(url,options)=>{
    if(url.endsWith('/twins/synthetic-twin/snapshots')) return result({items:['a','b'].map(id=>({snapshot_id:id,window:windowFor(id)})),total:2,has_more:false});
    return handler(url,options);
  },[{twin_id:'synthetic-twin',display_name:'Synthetic process'}]);
  await click(app.element('pt-twins').children[0].children[0]);
  return {...app,open:async id=>click(app.element('pt-snapshots').children[id==='a'?0:1].children[0])};
}

test('opening a snapshot without an index immediately clears previous history and pagination',async()=>{
  let resolveB;
  const app=await historyController(async url=>{
    if(url.endsWith('/snapshots/a')) return manifest('a');
    if(url.includes('/snapshots/a/timeline')) return page('a');
    if(url.endsWith('/snapshots/b')) return new Promise(resolve=>{resolveB=resolve;});
    if(url.includes('/evidence/')) return result({claim_class:'SYSTEM_DESCRIPTIVE',state:'AVAILABLE',content:'Synthetic A evidence'});
    throw new Error('Unexpected synthetic request: '+url);
  });
  await app.open('a');
  await click(app.element('pt-timeline').children[0].children[2]);
  assert.equal(app.element('pt-timeline').children.length,1);
  assert.ok(app.element('pt-detail').children.length);
  assert.equal(app.element('pt-more').hidden,false);
  app.element('pt-snapshots').children[1].children[0].events.click();
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.element('pt-detail').children.length,0);
  assert.equal(app.element('pt-more').hidden,true);
  assert.equal(app.element('pt-history').hidden,true);
  resolveB(manifest('b',true));await settle();
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.element('pt-more').hidden,true);
  assert.match(app.element('pt-scope').textContent,/1970-01-02/);
  assert.equal(app.calls.some(c=>c.url.includes('/snapshots/b/timeline')),false);
});

test('a late page from the previous snapshot cannot reappear under the next snapshot',async()=>{
  let resolvePage;
  const app=await historyController(async url=>{
    if(url.endsWith('/snapshots/a')) return manifest('a');
    if(url.includes('/snapshots/a/timeline')&&url.includes('cursor=')) return new Promise(resolve=>{resolvePage=resolve;});
    if(url.includes('/snapshots/a/timeline')) return page('a');
    if(url.endsWith('/snapshots/b')) return manifest('b',true);
    throw new Error('Unexpected synthetic request: '+url);
  });
  await app.open('a');
  app.element('pt-more').events.click();await settle();
  await app.open('b');
  resolvePage(page('late-a',false));await settle();
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.element('pt-more').hidden,true);
  assert.equal(app.element('pt-detail').children.length,0);
});

test('late evidence from the previous snapshot cannot populate the next snapshot',async()=>{
  let resolveDetail;
  const app=await historyController(async url=>{
    if(url.endsWith('/snapshots/a')) return manifest('a');
    if(url.includes('/snapshots/a/timeline')) return page('a');
    if(url.endsWith('/snapshots/b')) return manifest('b',true);
    if(url.includes('/evidence/')) return new Promise(resolve=>{resolveDetail=resolve;});
    throw new Error('Unexpected synthetic request: '+url);
  });
  await app.open('a');
  app.element('pt-timeline').children[0].children[2].events.click();await settle();
  await app.open('b');
  resolveDetail(result({claim_class:'SYSTEM_DESCRIPTIVE',state:'AVAILABLE',content:'Synthetic stale evidence'}));await settle();
  assert.equal(app.element('pt-detail').children.length,0);
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.element('pt-more').hidden,true);
});

test('a late manifest cannot replace the snapshot selected after it',async()=>{
  let resolveA;
  const app=await historyController(async url=>{
    if(url.endsWith('/snapshots/a')) return new Promise(resolve=>{resolveA=resolve;});
    if(url.endsWith('/snapshots/b')) return manifest('b',true);
    throw new Error('Unexpected synthetic request: '+url);
  });
  await app.open('a');
  await app.open('b');
  resolveA(manifest('a'));await settle();
  assert.match(app.element('pt-scope').textContent,/1970-01-02/);
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.calls.some(c=>c.url.includes('/snapshots/a/timeline')),false);
});

test('changing view clears history and ignores a late page even after changing back',async()=>{
  let resolvePage;
  const app=await historyController(async url=>{
    if(url.endsWith('/snapshots/a')) return manifest('a');
    if(url.includes('/timeline')&&url.includes('cursor=')) return new Promise(resolve=>{resolvePage=resolve;});
    if(url.includes('/timeline')) return page('current');
    throw new Error('Unexpected synthetic request: '+url);
  });
  await app.open('a');
  app.element('pt-more').events.click();await settle();
  app.element('pt-view').value='execution';
  app.element('pt-view').events.change();
  assert.equal(app.element('pt-timeline').children.length,0);
  assert.equal(app.element('pt-more').hidden,true);
  await settle();
  app.element('pt-view').value='episodes';
  app.element('pt-view').events.change();await settle();
  resolvePage(page('stale',false));await settle();
  assert.equal(app.element('pt-timeline').children.length,1);
  assert.match(app.element('pt-timeline').children[0].children[0].textContent,/Synthetic current/);
});
