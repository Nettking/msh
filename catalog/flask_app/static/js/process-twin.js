/* Private, explicit historical operations. No polling, external calls or storage. */
(() => {
  'use strict';
  const root = document.getElementById('process-twin');
  if (!root) return;
  const api = '/api/process-twins/v1';
  const el = id => document.getElementById(`pt-${id}`);
  const state = {inputs: [], prepared: null, twin: null, snapshot: null, cursor: null, pending: null, generation:0, historyGeneration:0};
  const labels = {history:'Prosesshistorie',operator_knowledge:'Operatørkunnskap',evidence:'Evidens',feedback:'Tilbakemelding',comparison:'Sammenligning',simulation:'Simulering'};
  const states = {AVAILABLE:'Tilgjengelig',PARTIAL:'Delvis',UNAVAILABLE:'Utilgjengelig',INSUFFICIENT_DATA:'Utilstrekkelig grunnlag',UNRESOLVED_CONTEXT:'Uavklart grunnlag'};
  const views = {episodes:'Episoder',execution:'Maskintilstander',gaps:'Kontinuitetsbrudd',operator:'Operatørregistreringer',legacy_daily:'Døgnoppsummeringer'};
  const reasons = {SOURCE_TIME_UNRESOLVED:'Kildens hendelsestid er uavklart.',SEMANTIC_ROLE_UNRESOLVED:'Maskinsignalenes rolle er uavklart.',EMPTY_WINDOW:'Ingen oppføringer i det vurderte tidsvinduet.',UTC_ASSUMED:'UTC er en uttrykkelig antakelse.',SEQUENCE_GAP:'Det bundne kildegrunnlaget inneholder sekvensbrudd.',TIMESTAMP_REGRESSION:'Kildeklokken har gått bakover.',PARTIAL_COVERAGE:'Deler av ønsket tidsvindu mangler dekning.',PARTIAL_SOURCE_BOUNDARY:'Kildens episodegrenser er delvise.',CONTEXT_UNRESOLVED:'Prosesskonteksten er ufullstendig.',DURATION_UNRESOLVED:'Varighet kan ikke bestemmes pålitelig.',METRIC_NOT_PROVIDED:'Kilden leverer ikke alle tidsmål.',OPERATOR_SOURCE_NOT_SELECTED:'Operatørgrunnlag er ikke valgt.',NO_VISIBLE_OPERATOR_RECORDS:'Ingen tilgjengelige operatørregistreringer inngår i utvalget.',UNLINKED_RECORDS:'Registreringen er ikke bekreftet knyttet til en episode.',RECORD_TIME_UNRESOLVED:'Registreringens hendelsestid er uavklart.',NO_EVIDENCE_IN_SCOPE:'Ingen påstander i utvalget har evidens å åpne.',SOURCE_UNAVAILABLE:'Eksakt kildegrunnlag er utilgjengelig.',NOT_IN_RELEASE:'Inngår ikke i historikkleveransen.'};
  const metricLabels = {requested_duration_us:'Ønsket tidsvindu (µs)',covered_duration_us:'Kildedekket tid (µs)',uncovered_duration_us:'Tid uten dekning (µs)',active_us:'Aktiv tid (µs)',program_stopped_us:'Programstopp (µs)',feed_hold_us:'Matehold (µs)',other_execution_us:'Andre maskintilstander (µs)',unknown_state_us:'Ukjent maskintilstand (µs)',active_share:'Aktivandel av dekket tid'};
  const describeReason = code => reasons[code] || code;
  function status(message, error=false) { el('status').textContent=message; el('status').dataset.error=error?'yes':'no'; }
  function clearHistory() {
    state.historyGeneration++;
    state.snapshot=null; state.cursor=null;
    for(const id of ['timeline','detail','scope','services','summary','view']) el(id).replaceChildren();
    for(const id of ['history','more','rebuild']) el(id).hidden=true;
    return state.historyGeneration;
  }
  function clearPrivate() {
    state.generation++;
    clearHistory();
    const pending=state.pending;
    state.prepared=state.twin=state.snapshot=null;
    state.inputs=[]; state.cursor=null;
    if(pending) pending.body=null;
    state.pending=pending;
    for (const id of ['twins','snapshots','timeline','detail','scope','services','summary','prepared']) if(el(id)) el(id).replaceChildren();
    root.querySelectorAll('input,textarea').forEach(node=>{node.value='';});
    for(const id of ['input','operator','view']) if(el(id)) el(id).replaceChildren(new Option('Ikke valgt',''));
    for(const id of ['more','more-twins','more-snapshots','command-status']) { el(id).hidden=true; el(id).onclick=null; }
    if(el('selected')) el('selected').textContent='Velg en tilgjengelig prosess på nytt.';
    el('command-status').hidden=!state.pending;
    el('history').hidden=true;
  }
  async function request(path, options={}) {
    const generation=state.generation;
    let response;
    try { response=await fetch(api+path,{credentials:'same-origin',cache:'no-store',referrerPolicy:'no-referrer',...options}); }
    catch (_error) { throw new Error('Forbindelsen ble brutt. Kontroller samme bestilling før en ny innsending.'); }
    const body=await response.json();
    if(generation!==state.generation) throw new Error('Valget er endret. Last tilgjengelig historikk på nytt.');
    if(!response.ok) {
      if([401,403,404].includes(response.status) || ['AUTHORIZATION_UNAVAILABLE','AUTHENTICATION_UNAVAILABLE'].includes(body.error?.code)) clearPrivate();
      const error=new Error(body.error?.message || 'Handlingen kunne ikke utføres.');
      error.code=body.error?.code;
      throw error;
    }
    return body;
  }
  function clearPending() { state.pending=null; el('command-status').hidden=true; }
  async function resolvePending() {
    const pending=state.pending;
    if(!pending) throw new Error('Ingen uavklart bestilling finnes på denne siden.');
    const result=(await request('/commands/by-key?operation='+pending.operation,{headers:{'Idempotency-Key':pending.key}})).data;
    if(result.status==='RUNNING') throw new Error('Bestillingen pågår. Kontroller status igjen før ny bestilling.');
    if(result.status==='FAILED') { clearPending(); throw new Error('Bestillingen feilet. Ingen ny bestilling er sendt.'); }
    const data=(await request(result.result_location.slice(api.length))).data;
    clearPending(); return {operation:pending.operation,data};
  }
  async function post(path, operation, payload) {
    const body=JSON.stringify(payload);
    if(state.pending) {
      if(state.pending.path!==path || state.pending.operation!==operation || state.pending.body!==body) throw new Error('Avklar den tidligere bestillingen med statusknappen før en ny bestilling.');
      return (await resolvePending()).data;
    }
    const key=crypto.randomUUID().replaceAll('-','');
    state.pending={path,operation,key,body}; el('command-status').hidden=false;
    const options={method:'POST',headers:{'Content-Type':'application/json','X-CSRFToken':root.dataset.csrf,'Idempotency-Key':key},body};
    try { const data=(await request(path,options)).data; clearPending(); return data; }
    catch(error) {
      // Resolve a lost response by its original key. Never create a new POST.
      if(!error.code || error.code==='COMMAND_RUNNING') {
        try { return (await resolvePending()).data; }
        catch(lookupError) { status(lookupError.message,true); throw lookupError; }
      } else clearPending();
      throw error;
    }
  }
  function button(label, action) {
    const node=document.createElement('button'); node.type='button'; node.textContent=label;
    node.addEventListener('click',()=>run(action,node)); return node;
  }
  async function run(action,node) {
    if(node) node.disabled=true;
    try { await action(); }
    catch(error) { status(error.message,true); }
    finally { if(node) node.disabled=false; }
  }
  async function refresh() {
    status('Henter tilgjengelige valg.');
    // Input availability and existing manifests are independent capabilities.
    let inputError=null;
    try {
      const inputData=await request('/inputs'); state.inputs=inputData.data.items;
      for(const id of ['input','operator']) {
        if(!el(id)) continue;
        el(id).replaceChildren(new Option(id==='input'?'Velg et tilgjengelig grunnlag':'Ikke valgt',''));
        state.inputs.filter(v=>(v.kind==='OPERATOR_RECORDS')===(id==='operator')).forEach(v=>{
          const option=new Option(v.display_name+(v.can_prepare?'':' — utilgjengelig'),v.input_id);
          option.disabled=!v.can_prepare; el(id).append(option);
        });
      }
    } catch(error) { inputError=error; }
    await loadTwins();
    status(inputError?inputError.message:'Tilgjengelige valg er oppdatert. Ingen analyse er startet.',!!inputError);
  }
  async function loadTwins(cursor=null) {
    const data=(await request('/twins'+(cursor?'?cursor='+encodeURIComponent(cursor):''))).data;
    if(!cursor) el('twins').replaceChildren();
    data.items.forEach(twin=>{
      const li=document.createElement('li'); li.append(button(twin.display_name,async()=>{
        state.twin=twin;
        clearHistory(); el('snapshots').replaceChildren(); el('more-snapshots').hidden=true;
        if(el('selected')) el('selected').textContent='Valgt prosess: '+twin.display_name;
        await loadSnapshots();
      })); el('twins').append(li);
    });
    el('more-twins').hidden=!data.has_more;
    el('more-twins').onclick=()=>run(()=>loadTwins(data.next_cursor),el('more-twins'));
    if(!data.total && !cursor) el('twins').textContent='Ingen tilgjengelig definisjon er registrert.';
  }
  async function loadSnapshots(cursor=null) {
    const twin=state.twin;
    const data=(await request(`/twins/${twin.twin_id}/snapshots`+(cursor?'?cursor='+encodeURIComponent(cursor):''))).data;
    if(state.twin!==twin) return;
    if(!cursor) el('snapshots').replaceChildren();
    data.items.forEach(snapshot=>{
      const li=document.createElement('li');
      li.append(button(snapshot.window.start_at+' – '+snapshot.window.end_at,()=>openSnapshot(snapshot.snapshot_id)));
      el('snapshots').append(li);
    });
    el('more-snapshots').hidden=!data.has_more;
    el('more-snapshots').onclick=()=>run(()=>loadSnapshots(data.next_cursor),el('more-snapshots'));
    if(!data.total && !cursor) el('snapshots').textContent='Ingen historiske snapshots er opprettet for denne prosessen.';
  }
  function term(list,label,value) { const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=label;dd.textContent=value;list.append(dt,dd); }
  async function openSnapshot(id) {
    const generation=clearHistory();
    const response=await request('/snapshots/'+id);
    if(generation!==state.historyGeneration) return;
    state.snapshot=response.data;
    const s=state.snapshot; state.cursor=null;
    el('scope').textContent=`Historisk vindu ${s.window.start_at} – ${s.window.end_at}. Modellgyldighet: ikke vurdert.`;
    el('services').replaceChildren(); el('summary').replaceChildren(); el('detail').replaceChildren();
    Object.entries(s.services).forEach(([name,value])=>term(el('services'),labels[name]||name,(states[value.state]||value.state)+(value.reason_codes.length?' · '+value.reason_codes.map(describeReason).join(' '):'')));
    Object.entries(s.summary).forEach(([name,value])=>term(el('summary'),metricLabels[name]||name,value===null?'Ukjent / ikke levert av kilden':name==='active_share'?(value*100).toLocaleString('nb-NO',{maximumFractionDigits:6})+' %':String(value)));
    el('view').replaceChildren(...s.available_views.map(v=>new Option(views[v]||v,v)));
    el('history').hidden=false;
    el('rebuild').hidden=!(root.dataset.analyze==='yes' && response.meta.technical_reason_codes.includes('VIEW_INDEX_UNAVAILABLE'));
    if(response.meta.technical_state==='PARTIAL') status('Historikken er bevart, men visningsgrunnlaget mangler.',true);
    else await loadTimeline();
  }
  async function showDetail(path) {
    const id=state.snapshot?.snapshot_id,generation=state.historyGeneration;
    if(!id || !path.startsWith(`/snapshots/${id}/`)) return;
    const data=(await request(path)).data;
    if(generation!==state.historyGeneration || id!==state.snapshot?.snapshot_id) return;
    const panel=el('detail'), list=document.createElement('dl');
    panel.replaceChildren(list);
    const valueText=value=>value===null?'Ukjent / ikke oppgitt':value===true?'Ja':value===false?'Nei':typeof value==='object'?JSON.stringify(value):String(value);
    if(data.row_kind==='EPISODE') {
      term(list,'Påstandstype','Systemets beskrivelse av eksisterende kildegrunnlag');
      term(list,'Original start',valueText(data.original_start_at)); term(list,'Original slutt',valueText(data.original_end_at));
      term(list,'Synlig start',valueText(data.visible_start_at)); term(list,'Synlig slutt',valueText(data.visible_end_at));
      term(list,'Delvis kildegrense ved start',valueText(data.partial_start)); term(list,'Delvis kildegrense ved slutt',valueText(data.partial_end));
      term(list,'Kildens episodeklassifikasjon',valueText(data.classification));
      term(list,'Grensebegrunnelser',valueText(data.boundary_reasons));
      for(const context of data.context.values) term(list,context.role,context.available?valueText(context.value):'Ukjent i kildegrunnlaget');
      term(list,'Uavklarte kontekstroller',data.context.pending_roles.join(', ')||'Ingen rapportert av kilden');
      for(const [key,value] of Object.entries(data.metrics||{})) term(list,metricLabels[key]||key,valueText(value));
    } else if(data.record_id) {
      const fieldLabels={observation:'Observasjon',trigger:'Utløsende forhold',context:'Kontekst',hypothesis:'Hypotese',goal:'Mål',decision:'Rapportert beslutning',rationale:'Begrunnelse',expected_outcome:'Forventet resultat',outcome:'Rapportert resultat',notes:'Notater',review_status:'Kildens vurderingsstatus',worked:'Kildens worked-felt',action_type:'Kildens handlingstype'};
      term(list,'Påstandstype','Menneskelig registrering; ingen målt effekt er utledet');
      term(list,'Hendelsestid',valueText(data.occurred_at)); term(list,'Registreringstid',valueText(data.recorded_at));
      term(list,'Tidsgrunnlag',({KNOWN:'Kjent',ASSUMED:'Antatt',UNRESOLVED:'Uavklart'})[data.time_status]);
      term(list,'Prosesskobling',({UNLINKED:'Ikke knyttet til en episode',UNRESOLVED:'Uavklart',PROPOSED:'Foreslått i kilden',CONFIRMED:'Bekreftet i kilden',REJECTED:'Avvist i kilden'})[data.link_status]);
      for(const [key,value] of Object.entries(data.fields)) term(list,fieldLabels[key]||key,data.field_states[key]==='MISSING'?'Ikke oppgitt':data.field_states[key]==='SOURCE_EMPTY'?'Tomt felt i kilden':valueText(value));
    } else {
      term(list,'Evidenstype',data.claim_class==='HUMAN_REPORTED'?'Menneskelig registrering':'Eksisterende maskin-/systemgrunnlag');
      term(list,'Tilgjengelighet',data.state==='AVAILABLE'?'Eksakt bevart innhold':'Eksakt grunnlag er utilgjengelig');
      const content=document.createElement('pre'); content.textContent=data.content===null?'Innholdet er utilgjengelig; dagens tekst er ikke brukt som reserve.':data.content; panel.append(content);
    }
    for(const ref of data.evidence_refs||[]) panel.append(button('Åpne tilhørende evidens',()=>showDetail(`/snapshots/${id}/evidence/${ref}`)));
    const technical=document.createElement('details'), heading=document.createElement('summary'), raw=document.createElement('pre');
    heading.textContent='Tekniske kildefelter og revisjoner'; raw.textContent=JSON.stringify(data,null,2); technical.append(heading,raw); panel.append(technical); panel.focus();
  }
  async function loadTimeline(cursor=null) {
    if(!state.snapshot) return;
    const id=state.snapshot.snapshot_id,view=el('view').value,generation=state.historyGeneration;
    const data=(await request(`/snapshots/${id}/timeline?view=${view}`+(cursor?'&cursor='+encodeURIComponent(cursor):''))).data;
    if(generation!==state.historyGeneration || id!==state.snapshot?.snapshot_id || view!==el('view').value) return;
    if(!cursor) { el('timeline').replaceChildren(); el('detail').replaceChildren(); }
    data.items.forEach(row=>{
      const li=document.createElement('li'),text=document.createElement('p');
      text.textContent=[row.classification||views[view],row.original_start_at||'Ukjent hendelsestid',...row.reason_codes.map(describeReason)].join(' · ');li.append(text);
      if(row.row_kind==='EPISODE') li.append(button('Åpne episode',()=>showDetail(`/snapshots/${id}/episodes/${row.row_id}`)));
      if(row.record_id) li.append(button('Åpne registrering',()=>showDetail(`/snapshots/${id}/records/${row.record_id}`)));
      row.evidence_refs.forEach(ref=>li.append(button('Åpne evidens',()=>showDetail(`/snapshots/${id}/evidence/${ref}`))));
      el('timeline').append(li);
    });
    el('more').hidden=!data.has_more; state.cursor=data.next_cursor;
    status(data.total===null?'Utvalget kan ikke tidsplasseres. Se oppgitt manglende grunnlag.':data.total===0?'Ingen oppføringer i det vurderte utvalget.':'Historikken er hentet fra det valgte snapshotet.');
  }
  function form(id,action) {
    if(!el(id)) return;
    el(id).addEventListener('submit',event=>{event.preventDefault();run(action,event.submitter);});
  }
  form('prepare',async()=>{
    const input=state.inputs.find(v=>v.input_id===el('input').value),operator=state.inputs.find(v=>v.input_id===el('operator').value);
    if(!input) throw new Error('Velg et tilgjengelig maskingrunnlag.');
    state.prepared=await post('/dataset-revisions','PREPARE_DATASET',{schema:'fcp.process_twin.prepare.v1',input_id:input.input_id,expected_binding_revision:input.binding_revision,operator_input_id:operator?.input_id||null,expected_operator_binding_revision:operator?.binding_revision||null,time_policy:el('time-policy').value});
    el('prepared').textContent='Datagrunnlaget er klargjort og revisjonen er bevart.'; status('Klargjøring er fullført.');
  });
  form('definition',async()=>{
    if(!state.prepared) throw new Error('Klargjør et datagrunnlag først.');
    const properties=['EXECUTION_HISTORY','OPERATOR_RECORDS','EVIDENCE'];
    if(state.prepared.kind==='CANONICAL_DEVICE') properties.push('OPERATIONAL_EPISODES');
    state.twin=await post('/twins','CREATE_TWIN',{schema:'fcp.process_twin.definition.v1',display_name:el('name').value,counterpart_kind:'PRODUCTION_PROCESS',counterpart_description:el('counterpart').value,process_boundary:el('boundary').value,dataset_revision_id:state.prepared.dataset_revision_id,goals:['PROCESS_HISTORY','PROVENANCE','DATA_QUALITY','OPERATOR_CONTEXT'],properties,role_profile:{observer:'SYSTEM',analyst:'SYSTEM_DESCRIPTIVE',decision_maker:'HUMAN',executor:'HUMAN'},validity_scope:el('validity').value});
    if(el('selected')) el('selected').textContent='Valgt prosess: '+state.twin.display_name;
    await loadTwins(); await loadSnapshots(); status('Definisjonen er registrert.');
  });
  form('build',async()=>{
    if(!state.twin) throw new Error('Velg en prosess først.');
    const snapshot=await post(`/twins/${state.twin.twin_id}/snapshots`,'BUILD_SNAPSHOT',{schema:'fcp.process_twin.snapshot_request.v1',definition_revision_id:state.twin.definition_revision_id,dataset_revision_ids:[state.prepared?.dataset_revision_id||state.twin.dataset_revision_id],window:{start_at:el('start').value,end_at:el('end').value},data_mode:'HISTORICAL'});
    await loadSnapshots(); await openSnapshot(snapshot.snapshot_id);
  });
  el('refresh').addEventListener('click',()=>run(refresh,el('refresh')));
  el('command-status').addEventListener('click',()=>run(async()=>{
    const result=await resolvePending(),data=result.data;
    if(result.operation==='PREPARE_DATASET') { state.prepared=data; if(el('prepared')) el('prepared').textContent='Tidligere bestilling er fullført; den klargjorte revisjonen er bevart.'; }
    else if(['CREATE_TWIN','REVISE_TWIN'].includes(result.operation)) { state.twin=data; await loadTwins(); await loadSnapshots(); }
    else if(['BUILD_SNAPSHOT','REBUILD_INDEX'].includes(result.operation)) await openSnapshot(data.snapshot_id);
    status('Bestillingsstatus er avklart. Ingen ny bestilling er sendt.');
  },el('command-status')));
  el('view').addEventListener('change',()=>run(()=>{
    state.historyGeneration++; state.cursor=null;
    el('timeline').replaceChildren(); el('detail').replaceChildren(); el('more').hidden=true;
    return loadTimeline();
  }));
  el('more').addEventListener('click',()=>run(()=>loadTimeline(state.cursor),el('more')));
  el('rebuild').addEventListener('click',()=>run(async()=>{
    await post(`/snapshots/${state.snapshot.snapshot_id}/rebuild-index`,'REBUILD_INDEX',{schema:'fcp.process_twin.rebuild.v1',expected_content_revision:state.snapshot.content_revision});
    await openSnapshot(state.snapshot.snapshot_id);
  },el('rebuild')));
  run(refresh);
})();
