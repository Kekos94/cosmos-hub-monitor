/* Charts use numerical approximations for display only; source CSV decimals stay intact. */
"use strict";
const Monitor = {
  number(value) { return value === "" || value == null || !Number.isFinite(Number(value)) ? null : Number(value); },
  filter(rows, period, last) {
    if (period === "all" || !last) return rows;
    const cutoff = new Date(last + "T00:00:00Z");
    cutoff.setUTCDate(cutoff.getUTCDate() - Number(period) + 1);
    return rows.filter(r => r.date >= cutoff.toISOString().slice(0, 10) && r.date <= last);
  },
  amount(value, locale='fr-FR', signed=false) {
    const n=Monitor.number(value);if(n===null)return null;
    return new Intl.NumberFormat(locale,{maximumFractionDigits:6,notation:n!==0&&Math.abs(n)<0.000001?'scientific':'standard',signDisplay:signed?'exceptZero':'auto'}).format(value);
  },
  latest(rows) { return rows.length ? [...rows].sort((a,b) => a.date.localeCompare(b.date)).at(-1) : null; },
  series(rows, key) {
    return [...rows].sort((a,b) => a.date.localeCompare(b.date)).map(r => ({date:r.date, value:Monitor.number(r[key])}));
  }
};
if (typeof module !== "undefined") module.exports = Monitor;

function startDashboard() {
  const data = JSON.parse(document.getElementById("monitor-data").textContent);
  const $ = id => document.getElementById(id);
  const language=document.documentElement.lang==='en'?'en':'fr';
  const locale=language==='en'?'en-GB':'fr-FR';
  const dictionary=JSON.parse($('monitor-i18n').textContent);
  const t=(text,params={})=>(language==='en'?(dictionary[text]??text):text).replace(/\{(\w+)\}/g,(_,key)=>params[key]??'{'+key+'}');
  const names = {cosmoshub:"Cosmos Hub", osmosis:"Osmosis", noble:"Noble", injective:"Injective"};
  const colors = {cosmoshub:"#315cf4", osmosis:"#8796ca", noble:"#0e8c82", injective:"#b7c4d7"};
  const fmt = (value, digits=2) => {
    const n = Monitor.number(value);
    return n === null ? t("Indisponible") : new Intl.NumberFormat(locale, {maximumFractionDigits:digits, minimumFractionDigits:digits}).format(n);
  };
  const day = value => value ? new Date(value+"T00:00:00Z").toLocaleDateString(locale, {day:"numeric",month:"short",year:"numeric",timeZone:"UTC"}) : t("Aucune journée");
  const esc = value => String(value).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const card = (label, value, unit, note, accent=false) => `<article class="card${accent ? " accent" : ""}"><p class="card-label">${esc(label)}</p><div class="card-value">${esc(value)} <small>${esc(unit)}</small></div><p class="card-note">${esc(note)}</p></article>`;
  const empty = (el, message=t("Aucune journée complète sur cette période.")) => { el.innerHTML = `<div class="chart-empty">${esc(message)}<span>${esc(t("Les données manquantes ne sont pas remplacées par zéro."))}</span></div>`; };
  const svgEl = (tag, attrs={}, text) => {
    const el = document.createElementNS("http://www.w3.org/2000/svg",tag);
    for (const [k,v] of Object.entries(attrs)) el.setAttribute(k,v);
    if (text != null) el.textContent = text;
    return el;
  };
  function lineChart(el, rows, key, unit) {
    const values = Monitor.series(rows,key), known = values.filter(p => p.value !== null);
    if (!known.length) return empty(el);
    const W=Math.max(300,el.clientWidth||760),H=260,L=77,R=25,T=20,B=42;
    const times = values.map(p => Date.parse(p.date+"T00:00:00Z"));
    const minX=Math.min(...times),maxX=Math.max(...times), maxY=Math.max(...known.map(p=>p.value),0), minY=Math.min(...known.map(p=>p.value),0);
    const spanY=maxY-minY || 1;
    const x=t => minX===maxX ? (L+W-R)/2 : L+(t-minX)/(maxX-minX)*(W-L-R);
    const y=v => H-B-(v-minY)/(spanY*1.15)*(H-T-B);
    const svg=svgEl("svg",{viewBox:`0 0 ${W} ${H}`,role:"img","aria-label":t("{count} observation(s), {unit}. Détails accessibles sur chaque point.",{count:known.length,unit})});
    for(let i=0;i<=3;i++) {
      const v=minY+spanY*1.15*i/3, py=y(v);
      svg.append(svgEl("line",{x1:L,x2:W-R,y1:py,y2:py,stroke:"#e8ecf3","stroke-dasharray":"3 5"}),svgEl("text",{x:L-12,y:py+4,"text-anchor":"end"},new Intl.NumberFormat(locale,{notation:"compact",maximumFractionDigits:2}).format(v)));
    }
    // Missing calendar days and missing values break a line; never interpolate a gap.
    let path="",previous=null;
    values.forEach(p => {
      if(p.value===null){ previous=null; return; }
      const t=Date.parse(p.date+"T00:00:00Z");
      path+=(previous!==null && t-previous===86400000 ? " L " : " M ")+x(t)+" "+y(p.value);
      previous=t;
    });
    svg.append(svgEl("path",{d:path,fill:"none",stroke:"#315cf4","stroke-width":2.5}));
    for(const p of known) {
      const label=`${day(p.date)} : ${fmt(p.value,4)} ${unit}`;
      const dot=svgEl("circle",{cx:x(Date.parse(p.date+"T00:00:00Z")),cy:y(p.value),r:5,fill:"#315cf4",stroke:"white","stroke-width":2,tabindex:0,"aria-label":label});
      dot.append(svgEl("title",{},label)); svg.append(dot);
    }
    const ticks=[...new Set([times[0],times[Math.floor((times.length-1)/2)],times.at(-1)])];
    for(const t of ticks) svg.append(svgEl("text",{x:x(t),y:H-12,"text-anchor":"middle"},new Date(t).toLocaleDateString(locale,{day:"numeric",month:"short",timeZone:"UTC"})));
    el.replaceChildren(svg);
    const note=document.createElement("p");note.className="chart-note";
    note.textContent=known.length===1 ? t("Une seule journée disponible : aucune tendance ne peut encore être établie.") : t("{count} journées renseignées · Les interruptions d’historique ne sont pas reliées.",{count:known.length});
    el.append(note);
  }
  function barChart(el, rows, key, incoming=false) {
    if(!rows.length) return empty(el,t("Aucune journée commune aux quatre chaînes."));
    const W=Math.max(320,el.clientWidth||680),H=260,L=100,R=75,T=20,step=53;
    const vals=rows.map(r=>Monitor.number(r[key])),max=Math.max(...vals.filter(v=>v!==null),1);
    const svg=svgEl("svg",{viewBox:`0 0 ${W} ${H}`,role:"img","aria-label":key==="sent_packets"?(incoming?t("Paquets reçus confirmés par chaîne"):t("Paquets émis par chaîne")):t("Sous-totaux USD valorisés par chaîne, valorisation potentiellement partielle")});
    rows.forEach((r,i)=>{
      const v=vals[i], py=T+i*step;
      svg.append(svgEl("text",{x:L-14,y:py+20,"text-anchor":"end"},names[r.chain]||r.chain));
      svg.append(svgEl("rect",{x:L,y:py,width:W-L-R,height:30,fill:"#f0f3f8",rx:4}));
      if(v!==null){
        const label=`${names[r.chain]||r.chain} : ${fmt(v,key==="sent_packets"?0:2)} ${key==="sent_packets"?t("paquets"):t("USD valorisés")}`;
        const bar=svgEl("rect",{x:L,y:py,width:Math.max(0,v)/max*(W-L-R),height:30,rx:4,fill:colors[r.chain]||"#8796ca",tabindex:0,"aria-label":label});bar.append(svgEl("title",{},label));svg.append(bar);
      }
      svg.append(svgEl("text",{x:W-R+12,y:py+20},v===null?t("ND"):new Intl.NumberFormat(locale,{notation:"compact",maximumFractionDigits:2}).format(v)));
    });
    el.replaceChildren(svg);
    const note=document.createElement("p");note.className="chart-note";
    note.textContent=key==="sent_packets"?t("Un paquet n’est ni un utilisateur distinct, ni un montant en dollars."):t("Sous-totaux historiques valorisés ; les actifs sans prix sont exclus, jamais comptés à zéro.");
    el.append(note);
  }
  const dates=[...data.criterion1,...data.criterion2,...(data.incoming||[]),...(data.net_chains||[])].map(r=>r.date).sort();
  const last=dates.at(-1);
  $("build-stamp").textContent=t("Page générée le ")+new Date(data.generated_at).toLocaleString(locale,{timeZone:"UTC",day:"numeric",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"})+" UTC";
  function render() {
    const incoming=$("flow-direction").value==='in';
    const normalize=r=>({...r,sent_packets:r.received_packets,valued_out_usd:r.valued_in_usd,hub_share_packet_count_pct:r.hub_share_confirmed_packet_count_pct});
    const flowData=incoming?(data.incoming||[]).map(normalize):data.criterion2;
    const chainData=incoming?(data.incoming_chains||[]).map(normalize):data.chains;
    const c1=Monitor.filter(data.criterion1,$("period").value,last),c2=Monitor.filter(flowData,$("period").value,last);
    $("ibc-description").textContent=incoming?t("Les entrées confirmées sont les réceptions ICS20 v1 avec un accusé de succès interprété dans la même transaction. Les erreurs et confirmations manquantes sont affichées séparément. Ce n’est pas une mesure du transit."):t("Comparaison des paquets ICS20 v1 émis par les quatre chaînes, sur une même journée. Les montants sont bruts ; le suivi des confirmations et remboursements est présenté séparément.");
    $("flow-title").textContent=incoming?t("Entrées confirmées par chaîne"):t("Flux sortants par chaîne");
    $("flow-metric").options[0].textContent=incoming?t("Paquets reçus confirmés"):t("Paquets émis");
    $("share-metric").options[0].textContent=incoming?t("Part des entrées confirmées (%)"):t("Part des paquets (%)");
    $("ibc-details").href='criterion2'+(language==='en'?'-en':'')+'.html'+(incoming?'#incoming':'');
    const a=Monitor.latest(c1),b=Monitor.latest(c2);
    $("c1-date").textContent=a ? day(a.date)+" · UTC" : t("Aucune journée dans la période");
    $("c2-date").textContent=b ? day(b.date)+t(" · UTC commun") : t("Aucune journée commune");
    const totalTx = Monitor.number(a?.tx_total);
    const ibcTx = Monitor.number(a?.tx_ibc);
    $("activity-cards").innerHTML =
      card(t("Transactions du Hub"), fmt(totalTx, 0), "", t("Journée UTC complète")) +
      card(t("Transactions IBC"), fmt(ibcTx, 0), "", t("Journée UTC complète")) +
      card(t("Part des transactions IBC"), fmt(totalTx > 0 && ibcTx !== null ? ibcTx / totalTx * 100 : null), "%", t("Parmi les transactions du Hub"));
    const feesKnown=a && a.fee_collection_status==="complete";
    $("c1-cards").innerHTML=card(t("Frais collectés"),fmt(feesKnown?a.collected_fee_atom:null,3),"ATOM",t("Tips inclus une seule fois"),true)+card(t("Frais IBC collectés"),fmt(feesKnown?a.collected_ibc_fee_atom:null,3),"ATOM",t("Inclus dans les frais collectés"))+card(t("Couverture de l’émission"),fmt(feesKnown?a.fee_to_estimated_emission_pct:null,4),"%",t("Frais / émission estimée"))+card(t("Émission estimée"),fmt(a?.estimated_emission_atom,0),"ATOM",t("Estimation sur les mêmes blocs"))+card(t("Couverture des blocs"),fmt(a?.block_coverage_pct,0),"%",a?fmt(a.block_count,0)+t(" blocs contrôlés"):t("Aucun total partiel extrapolé"));
    const metric=$("economy-metric").value;
    lineChart($("economy-chart"),c1.map(r=>({...r,[metric]:metric!=="estimated_emission_atom" && r.fee_collection_status!=="complete"?"":r[metric]})),metric,metric.endsWith("pct")?"%":"ATOM");
    $("c1-chart-caption").textContent=t("Journées UTC complètes · ")+$("economy-metric").selectedOptions[0].textContent;
    const n=b ? Monitor.number(b.sent_packets):null,p=b ? Monitor.number(b.priced_packets):null;
    const coverage=n>0 && p!==null ? p/n*100:null;
    $("c2-cards").innerHTML=card(incoming?t("Part du Hub · entrées confirmées"):t("Part du Hub · paquets"),fmt(b?.hub_share_packet_count_pct,2),"%",t("Dans les quatre chaînes"),true)+card(t("Part du Hub · USD valorisés"),fmt(b?.hub_share_valued_usd_pct,2),"%",t("Part du sous-ensemble valorisé"))+card(incoming?t("Entrées valorisées · quatre chaînes"):t("Flux valorisés · quatre chaînes"),fmt(b?.valued_out_usd,0),"USD",t("Sous-total, pas volume économique net"))+card(t("Paquets avec valorisation"),fmt(coverage,1),"%",b?fmt(b.unpriced_packets,0)+t(" paquets non valorisés"):t("Couverture en nombre, pas en valeur"));
    $("valuation-note").textContent=!b ? t("Aucune comparaison n’est publiée tant que les quatre index n’ont pas été parcourus sur une même journée UTC.") : b.valuation_status!=="complete" ? t("Valorisation partielle : {count} paquets sans valorisation. La part USD totale du Hub est indisponible. La couverture de {coverage} % porte sur le nombre de paquets, pas sur leur valeur.",{count:fmt(b.unpriced_packets,0),coverage:fmt(coverage,1)}) : t("Tous les paquets de cette journée disposent d’une valorisation de référence historique. Les montants restent des flux bruts émis, pas des flux nets réglés.");
    const chains=b?chainData.filter(r=>r.date===b.date && r.universe_id===b.universe_id).sort((a,b)=>Number(b.sent_packets)-Number(a.sent_packets)):[];
    $("chains-caption").textContent=b?day(b.date)+t(" UTC · Même journée pour les quatre chaînes"):t("Dernière journée commune sélectionnée");
    if(incoming && b) $("valuation-note").textContent=t("Entrées : {observed} réceptions observées, {confirmed} avec succès confirmé, {errors} avec accusé d’erreur et {unknown} sans confirmation interprétable dans la transaction de réception. {unpriced} entrées confirmées sans valorisation. Les parts portent sur le sous-ensemble confirmé ; aucun rapprochement de transit ou de solde net.",{observed:fmt(b.observed_packets,0),confirmed:fmt(b.received_packets,0),errors:fmt(b.error_ack_packets,0),unknown:fmt(b.unconfirmed_packets,0),unpriced:fmt(b.unpriced_packets,0)});
    barChart($("chains-chart"),chains,$("flow-metric").value,incoming);
    lineChart($("share-chart"),c2,$("share-metric").value,"%");
    const life=Monitor.filter((data.lifecycle||[]).filter(r=>r.direction===(incoming?'in':'out')),$("period").value,last);
    const latestLife=Monitor.latest(life),cohort=latestLife?life.filter(r=>r.date===latestLife.date):[];
    $("lifecycle-caption").textContent=latestLife?t("Cohorte du {date} UTC · {direction}",{date:day(latestLife.date),direction:incoming?t("Entrées observées"):t("Sorties émises")}):t("Aucune cohorte complète disponible");
    $("lifecycle-details").href='criterion2'+(language==='en'?'-en':'')+'.html#lifecycle';
    if(!cohort.length) empty($("lifecycle-table"),t("Aucune preuve de suivi disponible sur cette période."));
    else {
      const headings=[t("Chaîne"),t("Paquets suivis"),t("Succès confirmés"),t("Erreurs / timeouts"),t("Sans conclusion"),t("Remboursements vérifiés"),t("Recherche complète jusqu’au (UTC)")];
      $("lifecycle-table").innerHTML='<div class="lifecycle-scroll"><table><thead><tr>'+headings.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+cohort.map(r=>'<tr><th scope="row">'+esc(names[r.chain]||r.chain)+'</th><td>'+esc(fmt(r.tracked_packets,0))+'</td><td>'+esc(fmt(r.success_confirmed,0))+'</td><td>'+esc(fmt(Number(r.error_acknowledged)+Number(r.timeout_confirmed),0))+'<small>'+esc(t("{errors} erreurs · {timeouts} timeouts",{errors:fmt(r.error_acknowledged,0),timeouts:fmt(r.timeout_confirmed,0)}))+'</small></td><td>'+esc(fmt(Number(r.awaiting_evidence)+Number(r.unsupported_ack)+Number(r.conflicts),0))+'</td><td>'+esc(incoming?t("Source distante non vérifiée"):fmt(r.refunds_verified,0))+'</td><td>'+esc(r.coverage_complete==='true'?r.complete_through_utc.replace('T',' ').replace('Z',''):t("Recherche incomplète"))+(r.latest_evidence_day?'<small>'+esc(t("Dernière preuve : {date}",{date:day(r.latest_evidence_day)}))+'</small>':'')+'</td></tr>').join('')+'</tbody></table></div>';
    }
    $("lifecycle-note").textContent=t("Les cas sans conclusion restent inconnus. Les répétitions sans effet sont exclues ; un remboursement vérifié exige un mouvement bancaire correspondant. Une recherche complète ne signifie pas que tous les paquets sont résolus. Les séries brutes restent inchangées ; les flux nets sont présentés séparément ci-dessous.");

  }
  function renderNet() {
    const mode=$("net-window").value,rolling=mode!=='1';
    const dailyChains=Monitor.filter(data.net_chains||[],$("period").value,last);
    const dailyAssets=Monitor.filter(data.net_assets||[],$("period").value,last);
    const allChains=rolling?(data.net_window_chains||[]).filter(r=>String(r.window_days)===mode):dailyChains;
    const latestNet=Monitor.latest(rolling?allChains:allChains.filter(r=>r.coverage_status==='common_indexed_day'));
    const rows=latestNet?allChains.filter(r=>r.date===latestNet.date):[];
    const allAssets=rolling?(data.net_window_assets||[]).filter(r=>String(r.window_days)===mode):dailyAssets;
    const chain=$("net-chain").value;
    const choices=latestNet?allAssets.filter(r=>r.date===latestNet.date&&r.chain===chain):[];
    const old=$("net-asset").value;
    const label=r=>(r.symbol||t("Actif non identifié"))+' · '+(r.denom.length>40?r.denom.slice(0,21)+'…'+r.denom.slice(-9):r.denom);
    choices.sort((a,b)=>label(a).localeCompare(label(b)));
    $("net-asset").innerHTML=choices.map(r=>'<option value="'+esc(r.denom)+'">'+esc(label(r))+'</option>').join('');
    const native=choices.find(r=>r.metadata_status==='known'&&!r.denom.startsWith('ibc/')&&!r.denom.includes('/'));
    if(choices.some(r=>r.denom===old))$("net-asset").value=old;else if(native)$("net-asset").value=native.denom;
    const asset=choices.find(r=>r.denom===$("net-asset").value);
    const unit=asset?.symbol||t("unités inconnues");
    const amount=(v,signed=false)=>Monitor.amount(v,locale,signed)??t("Indisponible");
    const cutoff=latestNet?.evidence_through_utc?.replace('T',' ').replace('Z',' UTC')||t("Indisponible");
    $("net-caption").textContent=!latestNet?t("En attente d’une journée commune complète dans les deux sens"):rolling?t("Du {start} au {end} UTC · {covered}/{expected} jours couverts · preuves jusqu’au {cutoff}",{start:day(latestNet.start_date),end:day(latestNet.end_date),covered:latestNet.covered_days,expected:mode,cutoff}):t("Cohorte du {date} UTC · preuves communes jusqu’au {cutoff}",{date:day(latestNet.date),cutoff});
    $("net-details").href='criterion2'+(language==='en'?'-en':'')+'.html'+(rolling?'#windows':'#net-flows');
    const fullAsset=asset?.net_amount!==''&&asset?.net_amount!=null;
    const shownNet=rolling&&!fullAsset?asset?.observed_net_amount:asset?.net_amount;
    $("net-cards").innerHTML=card(rolling?t("Entrées réussies observées"):t("Entrées réussies"),amount(asset?.confirmed_in_amount),unit,rolling?t("Sur les jours couverts uniquement"):t("Réceptions de la journée, succès rapprochés"))+card(rolling?t("Sorties réussies observées"):t("Sorties réussies"),amount(asset?.confirmed_out_amount),unit,rolling?t("Sur les jours couverts uniquement"):t("Émissions de la journée, succès rapprochés"))+card(rolling&&!fullAsset?t("Solde sur les jours couverts"):t("Solde net de cet actif"),amount(shownNet,true),unit,rolling&&!fullAsset?t("Bilan complet de la période indisponible"):t("Entrées réussies − sorties réussies"),true)+(rolling?card(t("Couverture de la période"),latestNet?latestNet.covered_days+' / '+mode:t("Indisponible"),t("jours"),t("Jours manquants jamais remplacés par zéro")):card(t("Paquets incertains de cet actif"),fmt(asset?.unresolved_packets,0),t("paquets"),t("Ils bloquent le solde complet de l’actif")));
    $("net-coverage-note").hidden=!rolling;$("net-days").hidden=!rolling;
    if(rolling){
      $("net-coverage-note").textContent=latestNet?.coverage_status==='complete_window'?t("Tous les jours de la fenêtre sont couverts. Les confirmations et les prix manquants peuvent encore empêcher un bilan complet."):t("Historique incomplet : {covered}/{expected} jours couverts. Les chiffres sont les cumuls observés sur ces jours uniquement ; le bilan complet de la période reste indisponible. Aucune extrapolation.",{covered:latestNet?.covered_days||'0',expected:mode});
      if(latestNet?.evidence_status==='mixed_cutoffs')$("net-coverage-note").textContent=t("{covered}/{expected} jours couverts, mais les bornes de preuves diffèrent entre journées. Les montants cumulés restent indisponibles.",{covered:latestNet.covered_days,expected:mode});
      const currentDay=new Date();currentDay.setUTCDate(currentDay.getUTCDate()-1);
      if(latestNet&&latestNet.end_date<currentDay.toISOString().slice(0,10))$("net-coverage-note").textContent=t("Bilan en attente d’actualisation : il se termine le {date} UTC. Les dates affichées sont celles du dernier calcul disponible.",{date:day(latestNet.end_date)})+' '+$("net-coverage-note").textContent;
      const list=value=>value?value.split(';').map(day).join(', '):t("Aucun");
      $("net-days-detail").textContent=t("Jours couverts : {covered}. Jours manquants : {missing}.",{covered:list(latestNet?.covered_dates),missing:list(latestNet?.missing_dates)});
    }
    $("net-asset-note").textContent=asset?t("Dénomination locale : {denom}. {excluded} échecs exclus. Le prix historique ne sert qu’à la référence USD ; les montants en tokens restent séparés.",{denom:asset.denom,excluded:fmt(asset.excluded_packets,0)}):t("Aucun actif disponible dans une journée commune complète sur cette période.");
    if(rolling&&asset)$("net-asset-note").textContent+=' '+t("{count} paquets incertains pour cet actif.",{count:fmt(asset.unresolved_packets,0)});
    const headers=[t("Chaîne"),...(rolling?[t("Jours couverts")]:[]),t("Actifs résolus valorisés / suivis"),t("Entrées du sous-ensemble USD"),t("Sorties du sous-ensemble USD"),t("Solde du sous-ensemble USD"),t("Solde total USD")];
    if(!rows.length)empty($("net-summary"),t("Aucun solde n’est calculé tant que les deux sens et les index de preuves ne couvrent pas la même journée sur les quatre chaînes."));
    else $("net-summary").innerHTML='<div class="lifecycle-scroll"><table><thead><tr>'+headers.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr><th scope="row">'+esc(names[r.chain]||r.chain)+'</th>'+(rolling?'<td>'+esc(r.covered_days+' / '+mode)+'</td>':'')+'<td>'+esc(fmt(r.valued_assets,0)+' / '+fmt(r.assets,0))+'<small>'+esc(t("{count} paquets incertains",{count:fmt(r.unresolved_packets,0)}))+'</small></td><td>'+esc(fmt(r.valued_in_usd,2))+'</td><td>'+esc(fmt(r.valued_out_usd,2))+'</td><td>'+esc(fmt(r.valued_net_usd,2))+'</td><td>'+esc(fmt(r.total_net_usd,2))+'</td></tr>').join('')+'</tbody></table></div>';
    $("net-valuation-note").textContent=rolling?t("Les sous-totaux USD portent sur les jours couverts et sur les mêmes actifs résolus et valorisés dans les deux sens pendant toute la fenêtre. Chaque journée garde son prix historique. La couverture diffère entre chaînes ; le total de la période reste indisponible si un jour, un actif ou une confirmation manque."):t("Sous-totaux partiels : pour chaque chaîne, les entrées et sorties USD utilisent exactement les mêmes actifs résolus et valorisés. La couverture diffère entre chaînes ; ces soldes ne constituent pas un classement. Le total reste indisponible si des paquets, unités ou prix manquent. Un solde positif ne prouve pas une accumulation de capital.");
    if(asset?.metadata_status==='unknown')$("net-asset-note").textContent+=' '+t("Unités non identifiées : les montants bruts exacts restent disponibles dans le CSV.");
    if(asset?.metadata_status==='conflict')$("net-asset-note").textContent+=' '+t("Métadonnées contradictoires : conversion indisponible.");
    if(Number(rows.find(r=>r.chain===chain)?.unknown_payload_packets)>0)$("net-asset-note").textContent+=' '+t("Des paquets incertains sans actif identifié empêchent un solde complet.");
    const netHistory=asset?.metadata_status==='known'?dailyAssets.filter(r=>r.chain===chain&&r.denom===asset.denom&&String(r.decimals)===String(asset.decimals)&&r.coingecko_id===asset.coingecko_id):[];
    if(netHistory.some(r=>Monitor.number(r.net_amount)!==null))lineChart($("net-chart"),netHistory,'net_amount',unit);
    else empty($("net-chart"),t("Le solde n’est pas disponible pour cet actif sur cette période."));
  }
  function renderRwa() {
    const all = data.rwa || [];
    const balances = data.rwa_balances || [];
    const assetSelect = $("rwa-asset");
    const daySelect = $("rwa-day");
    const selectedAsset = assetSelect.value;
    const selectedDay = daySelect.value;
    const products = new Map([...all, ...balances].map(row => [row.product_id, row.symbol]));
    assetSelect.innerHTML = [...products].map(([id, symbol]) =>
      `<option value="${esc(id)}">${esc(symbol)}</option>`).join('');
    if (products.has(selectedAsset)) assetSelect.value = selectedAsset;
    const dates = [...new Set([...(data.net_chains || []), ...all, ...balances].map(row => row.date))].sort().reverse();
    daySelect.innerHTML = dates.map(date => `<option value="${esc(date)}">${esc(day(date))}</option>`).join('');
    if (dates.includes(selectedDay)) daySelect.value = selectedDay;
    else {
      const observedDay = dates.find(date => [...balances, ...all].some(row => row.date === date && row.product_id === assetSelect.value));
      if (observedDay) daySelect.value = observedDay;
    }
    const rows = all.filter(row => row.product_id === assetSelect.value && row.date === daySelect.value);
    const transit = rows.reduce((sum, row) => sum + Number(row.transit_packets), 0);
    $("rwa-transit").textContent = transit
      ? t("{count} transits PFM confirmés pour cet actif.", {count: fmt(transit, 0)})
      : t("Aucun transit PFM RWA vérifié pour cette sélection.");
    const amount = (value, symbol) => {
      const displayed = Monitor.amount(value, locale);
      return displayed === null ? t("Indisponible") : `${displayed} ${symbol}`;
    };
    const chainIds = {"cosmoshub-4": "Cosmos Hub", "osmosis-1": "Osmosis", "noble-1": "Noble", "injective-1": "Injective"};
    const chainName = row => chainIds[row.chain_id] || row.chain_id;
    const table = (headers, cells) => '<div class="lifecycle-scroll"><table><thead><tr>' +
      headers.map(label => `<th>${esc(t(label))}</th>`).join('') + '</tr></thead><tbody>' +
      cells.map(row => '<tr>' + row.map(value => `<td>${esc(value)}</td>`).join('') + '</tr>').join('') +
      '</tbody></table></div>';
    const stock = balances.filter(row => row.product_id === assetSelect.value && row.date === daySelect.value);
    if (stock.length) {
      $("rwa-balances").innerHTML = table(
        ["Chaîne", "Offre locale", "Escrow ICS20", "Offre hors escrow ICS20", "Bloc de clôture"],
        stock.map(row => [chainName(row), amount(row.supply_amount, row.symbol),
          amount(row.escrow_amount, row.symbol), amount(row.local_amount, row.symbol),
          row.height ? fmt(row.height, 0) : t("Indisponible")]),
      );
    } else {
      empty($("rwa-balances"), t("Aucun encours vérifié pour cette sélection."));
    }
    if (!rows.length) {
      empty($("rwa-results"), t("Aucune observation RWA identifiée pour cette sélection."));
      return;
    }
    const headers = ["Chaîne", "Montant entrant confirmé", "Montant sortant confirmé", "Solde vérifié", "Identités en attente", "Règlements en attente"];
    const cells = rows.map(row => [
      chainName(row), amount(row.confirmed_in, row.symbol),
      amount(row.confirmed_out, row.symbol), amount(row.net_amount, row.symbol),
      fmt(row.pending_identity, 0), fmt(row.unsettled, 0),
    ]);
    $("rwa-results").innerHTML = table(headers, cells);
  }
  function renderTransit() {
    const all=data.transit||[],latest=Monitor.latest(all);
    const rows=Monitor.filter(all,$("period").value,latest?.date).sort((a,b)=>b.date.localeCompare(a.date));
    const selected=$("transit-day").value;
    $("transit-day").innerHTML=rows.map(r=>`<option value="${esc(r.date)}">${esc(day(r.date))}</option>`).join('');
    if(rows.some(r=>r.date===selected))$("transit-day").value=selected;
    const row=rows.find(r=>r.date===$("transit-day").value);
    if(!row){empty($("transit-cards"));$("transit-coverage").textContent=t("Aucune preuve de transit disponible sur cette période.");$("transit-routes").innerHTML='';$("transit-assets").innerHTML='';return;}
    $("transit-cards").innerHTML=card(t("Transits confirmés"),fmt(row.confirmed_transit,0),t("paquets"),t("Succès entrant et sortant · chaînes distinctes"),true)
      +card(t("Premières émissions PFM liées"),fmt(row.linked_attempts,0),t("liens"),t("Échecs et cas indéterminés inclus"))
      +card(t("Échecs confirmés"),fmt(row.failed_forward,0),t("paquets"),t("Exclus des montants de transit"))
      +card(t("Liens indéterminés"),fmt(Number(row.unresolved)+Number(row.topology_unknown),0),t("paquets"),t("Règlement ou topologie sans preuve concluante"));
    const complete=row.collection_status==='candidate_proofs_complete';
    $("transit-coverage").textContent=t("{saved}/{total} transactions candidates contrôlées · {channels}/{expected} canaux historiques vérifiés · {received} réceptions observées sur le Hub.",{saved:fmt(row.inspected_transactions,0),total:fmt(row.candidate_transactions,0),channels:fmt(row.topology_proofs,0),expected:fmt(row.topology_expected,0),received:fmt(row.received_packets,0)})+' '+(complete?t("Preuves candidates complètes selon les index du fournisseur."):t("Collecte partielle : les nombres affichés sont uniquement ceux déjà prouvés."))+' '+t("Borne des confirmations : {date}.",{date:row.evidence_through_utc||t("Indisponible")})+' '+t("Retours à la source : {returns}. Réceptions sans lien prouvé : {unlinked} (statut de transit inconnu).",{returns:fmt(row.return_to_source,0),unlinked:fmt(Number(row.noncandidate_receipts)+Number(row.unlinked_candidate_receipts),0)})+(row.lower_bound_packet_share_pct!==''?' '+t("Part minimale identifiée des réceptions en nombre de paquets : {share} %.",{share:fmt(row.lower_bound_packet_share_pct,2)}):'');
    const table=(headers,records)=>'<div class="scroll"><table><thead><tr>'+headers.map(x=>'<th>'+esc(t(x))+'</th>').join('')+'</tr></thead><tbody>'+records.map(cells=>'<tr>'+cells.map(x=>'<td>'+esc(x)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
    const routes=(data.transit_routes||[]).filter(r=>r.date===row.date).sort((a,b)=>Number(b.confirmed_packets)-Number(a.confirmed_packets));
    $("transit-routes").innerHTML=routes.length?table(["Chaîne source","Via","Prochain saut","Transits confirmés","Échecs confirmés","Non résolus"],routes.map(r=>[r.source_chain,"Cosmos Hub",r.next_chain,fmt(r.confirmed_packets,0),fmt(r.failed_packets,0),fmt(r.unresolved_packets,0)])):t("Aucune route prouvée disponible.");
    const assets=(data.transit_assets||[]).filter(r=>r.date===row.date);
    $("transit-assets").innerHTML=assets.length?table(["Actif local","Transits confirmés","Montant confirmé","USD de référence"],assets.map(r=>[r.symbol||r.bank_denom,fmt(r.confirmed_packets,0),Monitor.amount(r.amount_display,locale)??t("Indisponible"),fmt(r.confirmed_usd,2)])):t("Aucun montant confirmé disponible.");
  }
  function statuses() {
    const yesterday=new Date();yesterday.setUTCDate(yesterday.getUTCDate()-1);const expected=yesterday.toISOString().slice(0,10);
    const status=(label,latest,detail)=>{
      const late=!latest || latest<expected;
      return `<div class="status-row"><div><b>${esc(label)}</b><small>${esc(detail)}</small></div><div class="status-value${late?" late":""}">${esc(day(latest))}<small>${latest ? late?t("Dernier jour publié · retard"):t("Dernier jour publié"):t("En attente de données")}</small></div></div>`;
    };
    const current1=Monitor.latest(data.criterion1_progress),incomplete1=current1?.status==="incomplete";
    let html=status(t("1A, 1C et 1D · Cosmos Hub"),Monitor.latest(data.criterion1)?.date,incomplete1?t("{date} en cours : {saved} / {expected} blocs",{date:day(current1.date),saved:fmt(current1.scanned_blocks,0),expected:fmt(current1.expected_blocks,0)}) : t("Couverture exhaustive des blocs du jour"));
    for(const chain of data.members) {
      const latest=Monitor.latest(data.criterion2_progress.filter(r=>r.chain===chain));
      const done=Monitor.latest(data.chains.filter(r=>r.chain===chain));
      const detail=latest && latest.coverage_status!=="indexed_complete" ? t("{date} en cours · {count} transactions enregistrées",{date:day(latest.date),count:fmt(latest.indexed_transactions_saved,0)}) : t("Couverture selon l’index d’événements");
      html+=status(t("1B · ")+(names[chain]||chain),done?.date,detail);
    }
    for(const chain of data.members) {
      const latest=Monitor.latest((data.incoming_progress||[]).filter(r=>r.chain===chain));
      const done=Monitor.latest((data.incoming_chains||[]).filter(r=>r.chain===chain));
      const detail=latest && latest.coverage_status!=="indexed_complete"?t("{date} en cours · {count} transactions enregistrées",{date:day(latest.date),count:fmt(latest.indexed_transactions_saved,0)}):t("Réceptions indexées ; accusés vérifiés séparément");
      html+=status(t("Entrées IBC · ")+(names[chain]||chain),done?.date,detail);
    }
    $("collection-status").innerHTML=html;
  }
  ["period","economy-metric","flow-direction","flow-metric","share-metric"].forEach(id=>$(id).addEventListener("change",render));
  let resizing;
  window.addEventListener("resize",()=>{clearTimeout(resizing);resizing=setTimeout(()=>{render();renderNet();},120);});
  ["period","net-chain","net-asset","net-window"].forEach(id=>$(id).addEventListener("change",renderNet));
  ["period","transit-day"].forEach(id=>$(id).addEventListener("change",renderTransit));
  ["rwa-asset","rwa-day"].forEach(id=>$(id).addEventListener("change",renderRwa));
  render();renderNet();renderRwa();renderTransit();statuses();
}
if (typeof document !== "undefined") {
  try { startDashboard(); } catch(error) {
    const box=document.getElementById("error");box.hidden=false;
    box.textContent=document.documentElement.lang==='en' ? "The dashboard cannot display this data. Detailed tables and downloads remain available below." : "Le dashboard ne peut pas afficher ces données. Les tableaux détaillés et téléchargements restent disponibles ci-dessous.";
    console.error("Dashboard rendering failed",error);
  }
}
