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
  replaceChildren(...items) { this.children=[...items]; this.textContent=''; }
  querySelectorAll() { return []; }
  focus() {}
}
const tick = () => new Promise(resolve=>setImmediate(resolve));
const result = data => ({ok:true,status:200,json:async()=>({data})});
async function controller(handler) {
  const elements=new Map();
  const element=id=>{ if(!elements.has(id)) elements.set(id,new Element()); return elements.get(id); };
  const calls=[];
  const fetch=async(url,options)=>{
    calls.push({url,options});
    if(url.endsWith('/inputs')) return result({items:[{input_id:'synthetic-input',binding_revision:'synthetic-revision',kind:'CANONICAL_DEVICE',can_prepare:true,display_name:'Synthetic'}]});
    if(url.endsWith('/twins')) return result({items:[],total:0,has_more:false});
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
