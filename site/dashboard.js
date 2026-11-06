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
  latest(rows) { return rows.length ? [...rows].sort((a,b) => a.date.localeCompare(b.date)).at(-1) : null; },
  series(rows, key) {
    return [...rows].sort((a,b) => a.date.localeCompare(b.date)).map(r => ({date:r.date, value:Monitor.number(r[key])}));
  }
};
if (typeof module !== "undefined") module.exports = Monitor;

function startDashboard() {
  const data = JSON.parse(document.getElementById("monitor-data").textContent);
  const $ = id => document.getElementById(id);
  const names = {cosmoshub:"Cosmos Hub", osmosis:"Osmosis", noble:"Noble", injective:"Injective"};
  const colors = {cosmoshub:"#315cf4", osmosis:"#8796ca", noble:"#0e8c82", injective:"#b7c4d7"};
  const fmt = (value, digits=2) => {
    const n = Monitor.number(value);
    return n === null ? "Indisponible" : new Intl.NumberFormat("fr-FR", {maximumFractionDigits:digits, minimumFractionDigits:digits}).format(n);
  };
  const day = value => value ? new Date(value+"T00:00:00Z").toLocaleDateString("fr-FR", {day:"numeric",month:"short",year:"numeric",timeZone:"UTC"}) : "Aucune journée";
  const esc = value => String(value).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const card = (label, value, unit, note, accent=false) => `<article class="card${accent ? " accent" : ""}"><p class="card-label">${esc(label)}</p><div class="card-value">${esc(value)} <small>${esc(unit)}</small></div><p class="card-note">${esc(note)}</p></article>`;
  const empty = (el, message="Aucune journée complète sur cette période.") => { el.innerHTML = `<div class="chart-empty">${esc(message)}<span>Les données manquantes ne sont pas remplacées par zéro.</span></div>`; };
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
    const svg=svgEl("svg",{viewBox:`0 0 ${W} ${H}`,role:"img","aria-label":`${known.length} observation(s), ${unit}. Détails accessibles sur chaque point.`});
    for(let i=0;i<=3;i++) {
      const v=minY+spanY*1.15*i/3, py=y(v);
      svg.append(svgEl("line",{x1:L,x2:W-R,y1:py,y2:py,stroke:"#e8ecf3","stroke-dasharray":"3 5"}),svgEl("text",{x:L-12,y:py+4,"text-anchor":"end"},new Intl.NumberFormat("fr-FR",{notation:"compact",maximumFractionDigits:2}).format(v)));
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
    for(const t of ticks) svg.append(svgEl("text",{x:x(t),y:H-12,"text-anchor":"middle"},new Date(t).toLocaleDateString("fr-FR",{day:"numeric",month:"short",timeZone:"UTC"})));
    el.replaceChildren(svg);
    const note=document.createElement("p");note.className="chart-note";
    note.textContent=known.length===1 ? "Une seule journée disponible : aucune tendance ne peut encore être établie." : `${known.length} journées renseignées · Les interruptions d’historique ne sont pas reliées.`;
    el.append(note);
  }
  function barChart(el, rows, key) {
    if(!rows.length) return empty(el,"Aucune journée commune aux quatre chaînes.");
    const W=Math.max(320,el.clientWidth||680),H=260,L=100,R=75,T=20,step=53;
    const vals=rows.map(r=>Monitor.number(r[key])),max=Math.max(...vals.filter(v=>v!==null),1);
    const svg=svgEl("svg",{viewBox:`0 0 ${W} ${H}`,role:"img","aria-label":key==="sent_packets"?"Paquets émis par chaîne":"Sous-totaux USD valorisés par chaîne, valorisation potentiellement partielle"});
    rows.forEach((r,i)=>{
      const v=vals[i], py=T+i*step;
      svg.append(svgEl("text",{x:L-14,y:py+20,"text-anchor":"end"},names[r.chain]||r.chain));
      svg.append(svgEl("rect",{x:L,y:py,width:W-L-R,height:30,fill:"#f0f3f8",rx:4}));
      if(v!==null){
        const label=`${names[r.chain]||r.chain} : ${fmt(v,key==="sent_packets"?0:2)} ${key==="sent_packets"?"paquets":"USD valorisés"}`;
        const bar=svgEl("rect",{x:L,y:py,width:Math.max(0,v)/max*(W-L-R),height:30,rx:4,fill:colors[r.chain]||"#8796ca",tabindex:0,"aria-label":label});bar.append(svgEl("title",{},label));svg.append(bar);
      }
      svg.append(svgEl("text",{x:W-R+12,y:py+20},v===null?"ND":new Intl.NumberFormat("fr-FR",{notation:"compact",maximumFractionDigits:2}).format(v)));
    });
    el.replaceChildren(svg);
    const note=document.createElement("p");note.className="chart-note";
    note.textContent=key==="sent_packets"?"Un paquet n’est ni un utilisateur distinct, ni un montant en dollars.":"Sous-totaux historiques valorisés ; les actifs sans prix sont exclus, jamais comptés à zéro.";
    el.append(note);
  }
  const dates=[...data.criterion1,...data.criterion2].map(r=>r.date).sort();
  const last=dates.at(-1);
  $("build-stamp").textContent="Page générée le "+new Date(data.generated_at).toLocaleString("fr-FR",{timeZone:"UTC",day:"numeric",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"})+" UTC";
  function render() {
    const c1=Monitor.filter(data.criterion1,$("period").value,last),c2=Monitor.filter(data.criterion2,$("period").value,last);
    const a=Monitor.latest(c1),b=Monitor.latest(c2);
    $("c1-date").textContent=a ? day(a.date)+" · UTC" : "Aucune journée dans la période";
    $("c2-date").textContent=b ? day(b.date)+" · UTC commun" : "Aucune journée commune";
    const feesKnown=a && a.fee_collection_status==="complete";
    $("c1-cards").innerHTML=card("Frais collectés",fmt(feesKnown?a.collected_fee_atom:null,3),"ATOM","Tips inclus une seule fois",true)+card("Couverture de l’émission",fmt(feesKnown?a.fee_to_estimated_emission_pct:null,4),"%","Fees / émission estimée")+card("Émission estimée",fmt(a?.estimated_emission_atom,0),"ATOM","Estimation sur les mêmes blocs")+card("Couverture des blocs",fmt(a?.block_coverage_pct,0),"%",a?fmt(a.block_count,0)+" blocs contrôlés":"Aucun total partiel extrapolé");
    const metric=$("economy-metric").value;
    lineChart($("economy-chart"),c1.map(r=>({...r,[metric]:metric!=="estimated_emission_atom" && r.fee_collection_status!=="complete"?"":r[metric]})),metric,metric.endsWith("pct")?"%":"ATOM");
    $("c1-chart-caption").textContent="Journées UTC complètes · "+$("economy-metric").selectedOptions[0].textContent;
    const n=b ? Monitor.number(b.sent_packets):null,p=b ? Monitor.number(b.priced_packets):null;
    const coverage=n>0 && p!==null ? p/n*100:null;
    $("c2-cards").innerHTML=card("Part du Hub · paquets",fmt(b?.hub_share_packet_count_pct,2),"%","Dans les quatre chaînes",true)+card("Part du Hub · USD valorisés",fmt(b?.hub_share_valued_usd_pct,2),"%","Part du sous-ensemble valorisé")+card("Flux valorisés · quatre chaînes",fmt(b?.valued_out_usd,0),"USD","Sous-total, pas volume économique net")+card("Paquets avec valorisation",fmt(coverage,1),"%",b?fmt(b.unpriced_packets,0)+" paquets non valorisés":"Couverture en nombre, pas en valeur");
    $("valuation-note").textContent=!b ? "Aucune comparaison n’est publiée tant que les quatre index n’ont pas été parcourus sur une même journée UTC." : b.valuation_status!=="complete" ? `Valorisation partielle : ${fmt(b.unpriced_packets,0)} paquets sans valorisation. La part USD totale du Hub est indisponible. La couverture de ${fmt(coverage,1)} % porte sur le nombre de paquets, pas sur leur valeur.` : "Tous les paquets de cette journée disposent d’une valorisation de référence historique. Les montants restent des flux bruts émis, pas des flux nets réglés.";
    const chains=b?data.chains.filter(r=>r.date===b.date && r.universe_id===b.universe_id).sort((a,b)=>Number(b.sent_packets)-Number(a.sent_packets)):[];
    $("chains-caption").textContent=b?day(b.date)+" UTC · Même journée pour les quatre chaînes":"Dernière journée commune sélectionnée";
    barChart($("chains-chart"),chains,$("flow-metric").value);
    lineChart($("share-chart"),c2,$("share-metric").value,"%");
  }
  function statuses() {
    const yesterday=new Date();yesterday.setUTCDate(yesterday.getUTCDate()-1);const expected=yesterday.toISOString().slice(0,10);
    const status=(label,latest,detail)=>{
      const late=!latest || latest<expected;
      return `<div class="status-row"><div><b>${esc(label)}</b><small>${esc(detail)}</small></div><div class="status-value${late?" late":""}">${esc(day(latest))}<small>${latest ? late?"Dernier jour publié · retard":"Dernier jour publié":"En attente de données"}</small></div></div>`;
    };
    const current1=Monitor.latest(data.criterion1_progress),incomplete1=current1?.status==="incomplete";
    let html=status("Critère 1 · Cosmos Hub",Monitor.latest(data.criterion1)?.date,incomplete1?`${day(current1.date)} en cours : ${fmt(current1.scanned_blocks,0)} / ${fmt(current1.expected_blocks,0)} blocs` : "Couverture exhaustive des blocs du jour");
    for(const chain of data.members) {
      const latest=Monitor.latest(data.criterion2_progress.filter(r=>r.chain===chain));
      const done=Monitor.latest(data.chains.filter(r=>r.chain===chain));
      const detail=latest && latest.coverage_status!=="indexed_complete" ? `${day(latest.date)} en cours · ${fmt(latest.indexed_transactions_saved,0)} transactions enregistrées` : "Couverture selon l’index d’événements";
      html+=status("Critère 2 · "+(names[chain]||chain),done?.date,detail);
    }
    $("collection-status").innerHTML=html;
  }
  ["period","economy-metric","flow-metric","share-metric"].forEach(id=>$(id).addEventListener("change",render));
  let resizing;
  window.addEventListener("resize",()=>{clearTimeout(resizing);resizing=setTimeout(render,120);});
  render();statuses();
}
if (typeof document !== "undefined") {
  try { startDashboard(); } catch(error) {
    const box=document.getElementById("error");box.hidden=false;
    box.textContent="Le dashboard ne peut pas afficher ces données. Les tableaux détaillés et téléchargements restent disponibles ci-dessous.";
    console.error("Dashboard rendering failed",error);
  }
}
