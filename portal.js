// API do Portal Sinequa (interno, atrás de login) — serve os blocos de dados do portal.
//
// Feijão com arroz:
//   · autenticação = 1 token compartilhado (header "x-portal-key" ou ?key=). Sem OAuth/sessão.
//   · dois backends com o MESMO SQL (ver dispatcher do mart() abaixo):
//       - Metabase export /json (db=3) — funciona de qualquer lugar via Tailscale, sem teto de linhas.
//       - Postgres direto (pg)         — mais robusto p/ produção; ativa com env de PG.
//   · cada rota devolve exatamente a forma que o front já consome (window.ANO, etc.),
//     então ligar no portal é trocar os const por fetch, sem mudar render.
//
// Rodar via Metabase/Tailscale (deste ambiente):
//   MB_URL=http://100.69.211.55:3000 PORTAL_TOKEN=sinequa2026 node portal.js
// Rodar via Postgres direto (no servidor, onde o pg é localhost):
//   npm install   # instala o pg (ver package.json)
//   PORTAL_DB=pg PGDATABASE=sinequa PORTAL_TOKEN=sinequa2026 node portal.js
//   (também aceita PGHOST/PGPORT/PGUSER/PGPASSWORD ou DATABASE_URL)
const http = require('http');
const path = require('path');
const fs   = require('fs');

// ---------- config ----------
function carregaEnv(arquivo){ try{ for(const l of fs.readFileSync(arquivo,'utf8').split('\n')){ const m=l.match(/^\s*([A-Z_]+)\s*=\s*(.*?)\s*$/); if(m && !l.trim().startsWith('#')) process.env[m[1]] ??= m[2].trim(); } }catch{} }
carregaEnv(path.join(__dirname,'..','sinequa-metabase','mb.env'));  // MB_API_KEY, MB_URL
carregaEnv(path.join(__dirname,'portal.env'));                      // opcional: PORTAL_TOKEN, MB_URL, PORTA

const PORTA  = Number(process.env.PORTAL_PORT || 8092);
const MB     = (process.env.MB_URL || 'http://localhost:3000').replace(/\/$/,'');
const MBKEY  = process.env.MB_API_KEY || '';
const TOKEN  = process.env.PORTAL_TOKEN || 'sinequa2026';
const ORIGENS = (process.env.PORTAL_ORIGENS || '*').split(',').map(s=>s.trim());
carregaEnv(path.join(__dirname,'ia.env'));                          // opcional: ANTHROPIC_API_KEY, CHAT_MODEL
const IA_KEY   = process.env.ANTHROPIC_API_KEY || '';
const IA_MODEL = process.env.CHAT_MODEL || 'claude-haiku-4-5-20251001';

// ---------- consulta ao mart — dois backends, MESMO SQL ----------
// · Metabase export /json (default): funciona de qualquer lugar via Tailscale, sem teto de 2000 linhas.
// · Postgres direto (pg): mais robusto p/ produção; ativa quando há env de PG (rodando no servidor).
// Toggle: PORTAL_DB=pg força pg · PORTAL_DB=mb força Metabase · sem isso, usa pg se houver PGHOST/DATABASE_URL.
const _pgEnv  = process.env.DATABASE_URL || process.env.PGHOST || process.env.PGDATABASE;
const USA_PG  = (process.env.PORTAL_DB||'').toLowerCase()==='pg' || (!process.env.PORTAL_DB && !!_pgEnv);
const BACKEND = USA_PG ? 'pg' : 'metabase';

let _pool=null;
function pool(){
  if(_pool) return _pool;
  const { Pool, types } = require('pg');
  // Faz o pg devolver os tipos como o Metabase/JSON: numeric→float, int8→int, date→'YYYY-MM-DD',
  // timestamp→string (nunca objeto Date). Assim os blocos abaixo não mudam uma linha.
  types.setTypeParser(1700, v=> v==null?null:parseFloat(v));   // numeric
  types.setTypeParser(20,   v=> v==null?null:parseInt(v,10));  // int8 / bigint
  types.setTypeParser(1082, v=> v);                            // date        -> 'YYYY-MM-DD'
  types.setTypeParser(1114, v=> v);                            // timestamp   -> string
  types.setTypeParser(1184, v=> v);                            // timestamptz -> string
  _pool = process.env.DATABASE_URL
    ? new Pool({ connectionString: process.env.DATABASE_URL, max: 4 })
    : new Pool({ host: process.env.PGHOST||'localhost', port: +process.env.PGPORT||5432,
                 database: process.env.PGDATABASE||'sinequa',
                 user: process.env.PGUSER||process.env.USER||require('os').userInfo().username,
                 password: process.env.PGPASSWORD, max: 4 });
  // um cliente ocioso que cai não pode derrubar o processo — só loga e o pool se recupera na próxima query
  _pool.on('error', e=>console.error('[pg pool]', e.message));
  return _pool;
}
async function martPg(sql){ const r = await pool().query(sql); return r.rows; }
async function martMb(sql){
  const body = new URLSearchParams({ query: JSON.stringify({type:'native',database:3,native:{query:sql}}) }).toString();
  const r = await fetch(MB+'/api/dataset/json',{ method:'POST',
    headers:{'x-api-key':MBKEY,'Content-Type':'application/x-www-form-urlencoded'},
    body, signal: AbortSignal.timeout(30000) });
  const t = await r.text();
  let j; try{ j = JSON.parse(t); }catch{ throw new Error('mart '+r.status+': '+t.slice(0,150)); }
  if(!Array.isArray(j)) throw new Error(j && j.error ? (''+j.error).slice(0,200) : 'resposta inesperada do mart');
  return j;
}
const mart = USA_PG ? martPg : martMb;
const N = v => v==null?0:Number(v);

// ---------- blocos ----------
// ANO — window.ANO = { meses:[{mes,v25,v26,o25,o26,b25,b26,c25,c26,d25,d26,no25,no26,nv25,nv26}], ativos:[{ativo,venda}], presc:{top,outros,total} }
async function blocoAno(){
  const [mens, ativos, presc] = await Promise.all([
    mart(`SELECT ano,mes,round(venda) v,round(orcado) o,round(venda_bruta) b,round(coalesce(custo,0)) c,round(desconto) d,n_orcamentos no,n_vendas nv FROM mart.mensal WHERE ano IN (2025,2026) ORDER BY ano,mes`),
    mart(`SELECT ativo_principal ativo, round(sum(venda)) venda FROM mart.venda_orcado_detalhe WHERE ano=2026 AND venda>0 GROUP BY 1 ORDER BY venda DESC LIMIT 12`),
    mart(`SELECT medico, round(sum(venda)) venda FROM mart.medico_diario WHERE ano=2026 GROUP BY 1 ORDER BY venda DESC`),
  ]);
  const meses = [];
  for(let m=1;m<=12;m++){
    const a = mens.find(r=>r.ano===2025 && r.mes===m) || {};
    const b = mens.find(r=>r.ano===2026 && r.mes===m) || {};
    meses.push({ mes:m,
      v25:N(a.v), v26:N(b.v), o25:N(a.o), o26:N(b.o), b25:N(a.b), b26:N(b.b),
      c25:N(a.c), c26:N(b.c), d25:N(a.d), d26:N(b.d),
      no25:N(a.no), no26:N(b.no), nv25:N(a.nv), nv26:N(b.nv) });
  }
  const top = presc.slice(0,10).map(r=>({medico:r.medico, venda:N(r.venda)}));
  const total = presc.reduce((s,r)=>s+N(r.venda),0);
  const outros = total - top.reduce((s,r)=>s+r.venda,0);
  return { meses, ativos: ativos.map(r=>({ativo:r.ativo, venda:N(r.venda)})), presc:{ top, outros, total } };
}

// MÊS — { TOT, DIACAL, ACUM, DIA, REM, SUPERMETA, PRESCR, fechado } — mês corrente OU um mês passado (?mes=YYYY-MM)
async function blocoMes(params){
  const mp = params && params.get ? params.get('mes') : null;
  const m = /^\d{4}-\d{2}$/.test(mp||'') ? mp : null;           // valida (evita injeção)
  const mesIni = m ? `date '${m}-01'` : `date_trunc('month',current_date)::date`;
  const fimMes = `(${mesIni} + interval '1 month' - interval '1 day')::date`;
  const refcut = `LEAST(current_date, ${fimMes})`;               // último dia realizado do mês (hoje, ou o fim se já fechou)
  const mfDiario = `date_trunc('month',data)=date_trunc('month',${mesIni})`;
  const [dias, meds, det, dscRows] = await Promise.all([
    mart(`SELECT to_char(data,'YYYY-MM-DD') iso, to_char(data,'DD') dd, (array['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'])[extract(dow from data)::int+1] dow,
            round(venda) venda, round(orcado) orcado, round(meta) meta, round(venda_bruta) vb,
            round(venda_acum) va, round(meta_acum) ma, (data<=${refcut}) ispast
          FROM mart.diario WHERE ${mfDiario} ORDER BY data`),
    mart(`SELECT medico m, round(sum(orcado)) orc, round(sum(venda)) vda,
            round((sum(desconto)/nullif(sum(venda_bruta),0))::numeric,4) descp
          FROM mart.medico_diario WHERE ${mfDiario} GROUP BY 1 ORDER BY vda DESC`),
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v
          FROM mart.venda_orcado_detalhe WHERE ano=extract(year from ${mesIni})::int AND mes=extract(month from ${mesIni})::int`),
    mart(`SELECT nrorc, round(sum(prcobr)) brt, round(sum(vrdsc)) dsc FROM mart.f_venda
          WHERE dtentr>=${mesIni} AND dtentr<=${refcut} AND nrorc>0 GROUP BY nrorc`),
  ]);
  const DIACAL = dias.map(d=>({dd:d.dd, dow:d.dow, venda:N(d.venda), orcado:N(d.orcado), meta:N(d.meta)}));
  const ACUM   = dias.map(d=>({dd:d.dd, va: d.ispast? N(d.va): null, ma: N(d.ma)}));
  const DIA    = DIACAL.filter(d=>d.orcado>0);
  const past = dias.filter(d=>d.ispast), fut = dias.filter(d=>!d.ispast);
  const fechado = fut.length===0;                                // mês inteiro já passou (nada de futuro)
  const v_liq = past.reduce((s,d)=>s+N(d.venda),0), v_bruta = past.reduce((s,d)=>s+N(d.vb),0);
  const orc = past.reduce((s,d)=>s+N(d.orcado),0);
  const meta_ate = past.reduce((s,d)=>s+N(d.meta),0), meta_mes = dias.reduce((s,d)=>s+N(d.meta),0);
  const ultDia = [...past].reverse().find(d=>N(d.venda)>0);
  const ref = ultDia? ultDia.iso : (dias.length? dias[dias.length-1].iso : null);
  const TOT = { v_liq, v_bruta, orc, meta_ate, meta_mes, ref };
  const SUPERMETA = Math.round(meta_mes*1.065);
  const REM = { falta: Math.max(meta_mes - v_liq,0), dias: fut.length, dias_uteis: fut.filter(d=>N(d.meta)>0).length };
  const medicos = meds.map(r=>({m:r.m, orc:N(r.orc), vda:N(r.vda), desc:N(r.descp)}));
  const idx={}; medicos.forEach((x,i)=>idx[x.m]=i);
  const detArr = det.filter(r=>idx[r.medico]!=null).map(r=>[idx[r.medico], r.paciente, N(r.nr), r.ativo, N(r.o), N(r.v)]);
  const dsc={}; dscRows.forEach(r=>{ dsc[N(r.nrorc)]=[N(r.brt), N(r.dsc)]; });
  const PRESCR = { ref, medicos, det: detArr, dsc };
  return { TOT, DIACAL, ACUM, DIA, REM, SUPERMETA, PRESCR, fechado };
}

// DIA — window.HOJE = { hoje, dias:[{d,lbl}], byDay:{ 'YYYY-MM-DD': {ag:{venda,orcado,meta,bruta,dow}, nped, medicos:[{m,vda,brt,dsc}], det:[[mi,pac,nr,ativo,orc,vda]], dsc:{nr:[brt,dsc]}} } }
// Datas do mart vêm como "YYYY-MM-DD" (string). Fatia direto — NUNCA new Date(str), que interpreta
// como UTC e desloca 1 dia no fuso do servidor (BRT). Só cai no Date() se vier um objeto Date mesmo.
function _iso(d){ if(typeof d==='string') return d.slice(0,10); const x=new Date(d); return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); }
async function blocoDia(params){
  // dia de referência: ?d=YYYY-MM-DD (qualquer dia passado) ou hoje (current_date). Janela de 25 dias até ele.
  const dp = params && params.get ? params.get('d') : null;
  const dref = /^\d{4}-\d{2}-\d{2}$/.test(dp||'') ? dp : null;
  const A = dref ? `date '${dref}'` : 'current_date';
  const [forc, vod, diario] = await Promise.all([
    mart(`SELECT dtentr::date d, nrorc, round(sum(prcobr)) brt, round(sum(vrdsc)) dsc FROM mart.f_venda WHERE dtentr>=${A}-25 AND dtentr<=${A} AND nrorc>0 GROUP BY 1,2`),
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v FROM mart.venda_orcado_detalhe
          WHERE nr_orcamento IN (SELECT DISTINCT nrorc FROM mart.f_venda WHERE dtentr>=${A}-25 AND dtentr<=${A} AND nrorc>0)`),
    mart(`SELECT data::date d, (data=${A}) ishoje, (array['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'])[extract(dow from data)::int+1] dow, round(venda) venda, round(orcado) orcado, round(meta) meta, round(venda_bruta) bruta FROM mart.diario WHERE data>=${A}-25 AND data<=${A} ORDER BY 1 DESC`),
  ]);
  // "hoje" = a data REAL de hoje (mart.diario tem a linha do dia mesmo sem venda), não o último dia com venda.
  const diarioBy = {}; let hoje=null;
  diario.forEach(r=>{ const d=_iso(r.d); diarioBy[d]={venda:N(r.venda),orcado:N(r.orcado),meta:N(r.meta),bruta:N(r.bruta),dow:r.dow}; if(r.ishoje) hoje=d; });
  if(!hoje && diario.length) hoje=_iso(diario[0].d);   // fallback: dia mais recente
  // dias no seletor: os que tiveram movimento (orçou ou vendeu) + SEMPRE o de hoje — os 11 mais recentes
  const dayList = diario.filter(r=> N(r.orcado)>0 || N(r.venda)>0 || r.ishoje).slice(0,11).map(r=>_iso(r.d));
  const npedBy = {}; forc.forEach(r=>{ const d=_iso(r.d); npedBy[d]=(npedBy[d]||0)+1; });
  // f_venda: nrorc -> {day, brt, dsc}
  const forcByNr = {}; forc.forEach(r=>{ forcByNr[N(r.nrorc)]={day:_iso(r.d), brt:N(r.brt), dsc:N(r.dsc)}; });
  // vod agrupado por nrorc
  const vodByNr = {}; vod.forEach(r=>{ (vodByNr[N(r.nr)] ||= []).push({medico:r.medico, paciente:r.paciente, ativo:r.ativo, o:N(r.o), v:N(r.v)}); });
  const byDay = {};
  for(const day of dayList){
    const nrs = forc.filter(r=>_iso(r.d)===day).map(r=>N(r.nrorc));
    const medMap = {}, dsc = {}, orders = [];
    for(const nr of nrs){
      const f = forcByNr[nr] || {brt:0,dsc:0}; dsc[nr]=[f.brt, f.dsc];
      const rows = vodByNr[nr] || [];
      const med = rows[0]?.medico || '—', pac = rows[0]?.paciente || '—';
      const m = (medMap[med] ||= {m:med, vda:0, brt:0, dsc:0});
      m.brt += f.brt; m.dsc += f.dsc;
      rows.forEach(x=>{ m.vda += x.v; });
      orders.push({nr, pac, rows});
    }
    const medicos = Object.values(medMap).sort((a,b)=>b.vda-a.vda);
    const idx = {}; medicos.forEach((x,i)=>idx[x.m]=i);
    const det = [];
    orders.forEach(o=>{ o.rows.forEach(x=>{ const mi=idx[x.medico]; if(mi!=null) det.push([mi, o.pac, o.nr, x.ativo, x.o, x.v]); }); });
    byDay[day] = { ag: diarioBy[day] || {venda:0,orcado:0,meta:0,bruta:0,dow:''}, nped: npedBy[day]||nrs.length, medicos, det, dsc };
  }
  const dias = dayList.map(d=>({d, lbl: d.slice(8)+'/'+d.slice(5,7)}));
  return { hoje: dayList[0], dias, byDay };
}

// PRODUÇÃO — window.PROD = { hoje, pcpHoje, ref, semanas:[{wk,lbl,prazo,atraso,ematraso,producao,dias:[{dow,dd,prazo,atraso,ematraso,producao}]}], atrByWk:{wk:[...]}, pcpDias:[{d,lbl}], pcpByDia:{dia:[{ped,paciente,envio,pend,stMin,formulas:[{serier,ativo,etapa,st}]}]} }
const _DOW=['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
function _monday(d){ const x=new Date(d+'T00:00:00'); const g=x.getDay(); x.setDate(x.getDate()-((g+6)%7)); return _isoD(x); }
function _isoD(x){ return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); }
function _ddmm(d){ return d.slice(8)+'/'+d.slice(5,7); }
const _STmin={'Em atraso':0,'Em produção':1,'Pronta com Atraso':2,'Pronta no Prazo':3};
async function blocoProd(){
  const [sem, atr, fila] = await Promise.all([
    mart(`SELECT data_prevista_entrega::date d, status_entrega st, count(*) n FROM mart.pcp
          WHERE data_prevista_entrega >= current_date - interval '8 weeks' AND data_prevista_entrega < current_date + interval '2 weeks' GROUP BY 1,2`),
    mart(`SELECT data_prevista_entrega::date prev, nrrqu ped, chave_formula chave, paciente, to_char(data_hora_saida_lab,'YYYY-MM-DD HH24:MI') saida, tipo_envio envio, status_entrega st
          FROM mart.pcp WHERE status_entrega IN ('Pronta com Atraso','Em atraso') AND data_prevista_entrega >= current_date - interval '8 weeks'`),
    mart(`SELECT data_prevista_entrega::date d, nrrqu ped, serier, paciente, tipo_envio envio, situacao etapa, status_entrega st
          FROM mart.pcp WHERE data_prevista_entrega >= current_date - interval '12 days' AND data_prevista_entrega < current_date + interval '6 days' ORDER BY data_prevista_entrega, nrrqu, serier`),
  ]);
  // ativos por pedido (nrrqu -> [ativo...]) só dos pedidos da fila
  const pedSet=[...new Set(fila.map(r=>N(r.ped)))];
  let ativoByPed={};
  if(pedSet.length){
    const av=await mart(`SELECT f.nrrqu ped, v.ativo_principal ativo FROM mart.f_venda f JOIN mart.venda_orcado_detalhe v ON v.nr_orcamento=f.nrorc
                         WHERE f.nrrqu IN (${pedSet.join(',')})`);
    av.forEach(r=>{ (ativoByPed[N(r.ped)] ||= []).push(r.ativo); });
  }
  // ---- semanas ----
  const dayAgg={}; // d -> {prazo,atraso,ematraso,producao}
  sem.forEach(r=>{ const d=_iso(r.d); const o=(dayAgg[d] ||= {prazo:0,atraso:0,ematraso:0,producao:0});
    if(r.st==='Pronta no Prazo')o.prazo+=N(r.n); else if(r.st==='Pronta com Atraso')o.atraso+=N(r.n); else if(r.st==='Em atraso')o.ematraso+=N(r.n); else if(r.st==='Em produção')o.producao+=N(r.n); });
  const wkMap={};
  Object.keys(dayAgg).forEach(d=>{ const wk=_monday(d); (wkMap[wk] ||= {}); wkMap[wk][d]=dayAgg[d]; });
  const semanas = Object.keys(wkMap).sort().map(wk=>{
    const dias=[]; let prazo=0,atraso=0,ematraso=0,producao=0;
    for(let i=0;i<7;i++){ const dt=new Date(wk+'T00:00:00'); dt.setDate(dt.getDate()+i); const d=_isoD(dt); const a=wkMap[wk][d]||{prazo:0,atraso:0,ematraso:0,producao:0};
      prazo+=a.prazo;atraso+=a.atraso;ematraso+=a.ematraso;producao+=a.producao;
      dias.push({dow:_DOW[dt.getDay()], dd:String(dt.getDate()).padStart(2,'0'), prazo:a.prazo,atraso:a.atraso,ematraso:a.ematraso,producao:a.producao}); }
    const end=new Date(wk+'T00:00:00'); end.setDate(end.getDate()+6);
    return { wk, lbl:_ddmm(wk)+'–'+_ddmm(_isoD(end)), prazo,atraso,ematraso,producao, dias };
  });
  const hoje = _monday(_isoD(new Date()));
  // ---- atrByWk ----
  const atrByWk={}; atr.forEach(r=>{ const wk=_monday(_iso(r.prev)); (atrByWk[wk] ||= []).push({ped:N(r.ped),chave:r.chave,paciente:r.paciente,prev:_iso(r.prev),saida:r.saida,envio:r.envio,st:r.st}); });
  // ---- pcpByDia ----
  const pcpByDia={}, daysSeen=new Set();
  const byDayPed={}; // d -> ped -> {paciente,envio,formulas:[]}
  fila.forEach(r=>{ const d=_iso(r.d); daysSeen.add(d); const ped=N(r.ped);
    const dm=(byDayPed[d] ||= {}); const p=(dm[ped] ||= {ped,paciente:r.paciente,envio:r.envio,formulas:[]});
    p.formulas.push({serier:N(r.serier), etapa:r.etapa, st:r.st}); });
  Object.keys(byDayPed).forEach(d=>{
    pcpByDia[d]=Object.values(byDayPed[d]).map(p=>{
      const av=ativoByPed[p.ped]||[];
      p.formulas.forEach((f,i)=>{ f.ativo = av[i] || av[0] || '—'; });
      const pend=p.formulas.filter(f=>f.st==='Em produção'||f.st==='Em atraso').length;
      const stMin=Math.min(...p.formulas.map(f=>_STmin[f.st]!=null?_STmin[f.st]:3));
      return {ped:p.ped, paciente:p.paciente, envio:p.envio, pend, stMin, formulas:p.formulas};
    }).sort((a,b)=>a.stMin-b.stMin);
  });
  const pcpDiasAll=[...daysSeen].filter(d=>d<=_isoD(new Date())).sort();
  const pcpDias = pcpDiasAll.map(d=>({d, lbl:_ddmm(d)+' '+_DOW[new Date(d+'T00:00:00').getDay()]}));
  const pcpHoje = pcpDias.length? pcpDias[pcpDias.length-1].d : _isoD(new Date());
  return { hoje, pcpHoje, ref: _isoD(new Date()), semanas, atrByWk, pcpDias, pcpByDia };
}

// BUSCA — window.BUSCA = { ref, pacientes:[{nome,nped,orcado,vendido,ultima,medicos:[],cadastro:{cpf,rg:{num,orgao,uf},email,nascimento,cliente_desde,endereco:{...}},orcs:[{nr,data,dorc,medico,orcado,vendido,ativos:[{ativo,orcado,vendido}],desc}]}] }
// cadastro sai do stg.dim_cliente (join por CDCLI, via pedido: f_venda/f_orcamento.nr→cdcli).
// Telefone NÃO vem (o Fórmula não guarda — fica no CRM). No portal ao vivo (atrás de login) vem completo.
function _cad(r){
  if(!r) return null;
  const cep = r.nrcep ? String(r.nrcep).replace(/\D/g,'').replace(/^(\d{5})(\d{3})$/,'$1-$2') : null;
  const temEnd = r.ender||r.bairr||r.munic;
  return {
    cpf: r.nrcnpj||null,
    rg: r.nrinscr ? { num:r.nrinscr, orgao:r.oerg||null, uf:r.ufrg||null } : null,
    email: r.email||null,
    nascimento: r.dtnas||null,
    cliente_desde: r.dtcad||null,
    endereco: temEnd ? { logradouro:r.ender||null, numero:r.endnr||null, complemento:r.endcp||null,
                         bairro:r.bairr||null, cep, cidade:r.munic||null, uf:r.unfed||null } : null
  };
}
async function blocoBusca(){
  const [vod, fv, fo] = await Promise.all([
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v FROM mart.venda_orcado_detalhe WHERE ano=extract(year from current_date)::int`),
    mart(`SELECT nrorc nr, max(cdcli) cdcli, to_char(max(dtentr),'YYYY-MM-DD') data, round(sum(prcobr)) brt, round(sum(vrdsc)) dscv FROM mart.f_venda WHERE nrorc>0 AND dtentr>=date_trunc('year',current_date) GROUP BY nrorc`),
    mart(`SELECT nrorc nr, max(cdcli) cdcli, to_char(max(dtentr),'YYYY-MM-DD') dorc FROM mart.f_orcamento WHERE nrorc>0 AND dtentr>=date_trunc('year',current_date) GROUP BY nrorc`),
  ]);
  // cadastro é opcional: se o extract ainda não trouxe as colunas novas (RG/endereço), degrada sem quebrar o busca.
  let cad=[];
  try{ cad = await mart(`SELECT cdcli, nrcnpj, nrinscr, oerg, ufrg, email, to_char(dtnas,'YYYY-MM-DD') dtnas, to_char(dtcad,'YYYY-MM-DD') dtcad, ender, endnr, endcp, bairr, nrcep, munic, unfed FROM stg.dim_cliente`); }
  catch(e){ console.warn('[busca] cadastro indisponível — '+e.message.slice(0,80)); }
  const fvMap={}; fv.forEach(r=>{ fvMap[N(r.nr)]={data:r.data, brt:N(r.brt), dscv:N(r.dscv), cdcli:N(r.cdcli)}; });
  const foMap={}; fo.forEach(r=>{ foMap[N(r.nr)]={dorc:r.dorc, cdcli:N(r.cdcli)}; });
  const cadMap={}; cad.forEach(r=>{ cadMap[N(r.cdcli)]=r; });
  const pacMap={};
  vod.forEach(r=>{
    const nome=r.paciente||'—'; const p=(pacMap[nome] ||= {nome, orders:{}, medicos:new Set(), cdcli:null});
    const nr=N(r.nr); const ord=(p.orders[nr] ||= {nr, medico:r.medico, orcado:0, vendido:0, ativos:[]});
    ord.orcado+=N(r.o); ord.vendido+=N(r.v); ord.ativos.push({ativo:r.ativo, orcado:N(r.o), vendido:N(r.v)});
    if(r.medico) p.medicos.add(r.medico);
    if(!p.cdcli){ const c=(fvMap[nr]||{}).cdcli || (foMap[nr]||{}).cdcli; if(c) p.cdcli=c; }
  });
  const pacientes = Object.values(pacMap).map(p=>{
    const orcs = Object.values(p.orders).map(o=>{
      const f=fvMap[o.nr]||{};
      return { nr:o.nr, data:f.data||null, dorc:(foMap[o.nr]||{}).dorc||f.data||null, medico:o.medico, orcado:o.orcado, vendido:o.vendido,
               ativos:o.ativos, desc: (f.brt? Math.round(f.dscv/f.brt*1e4)/1e4 : null) };
    }).sort((a,b)=> (b.data||b.dorc||'').localeCompare(a.data||a.dorc||''));
    const vendido=orcs.reduce((s,o)=>s+o.vendido,0), orcado=orcs.reduce((s,o)=>s+o.orcado,0);
    const ultima=orcs.map(o=>o.data).filter(Boolean).sort().at(-1)||null;
    return { nome:p.nome, nped:orcs.length, orcado, vendido, ultima, medicos:[...p.medicos], cadastro:_cad(cadMap[p.cdcli]), orcs };
  }).sort((a,b)=>b.vendido-a.vendido);
  return { ref: new Date().toISOString().slice(0,10), pacientes };
}

// ============================================================
// ESTOQUE — window.ESTOQUE (módulo Estoque do portal). Lê a camada mart de estoque
// (sinequa-fb/mart-estoque.sql: estoque_sku, estoque_lote, estoque_ajuste, compra,
// estoque_snapshot) e as linhas de consumo (stg.venda_componente).
//
// Decisões — não reverter sem motivo:
// · Estoque ÓTIMO é calculado no FRONT (prazo e ciclo são ajustáveis na tela). Aqui só
//   vão os insumos: consumo/dia (90d), desvio/dia (a partir do consumo SEMANAL de 13
//   semanas, com semana zerada contando), z pela curva (A 95% · B 90% · C 85%), validade
//   média dos lotes comprados (o máximo não pode passar do que se usa antes de vencer)
//   e o último preço pago.
// · CUSTO REAL = soma de prcusto (custo R$ da linha, já com a perda — conferido no
//   Peptistrong: (72+3,6)g × 1,18 = 89,21). Linha é SUSPEITA quando custa > R$ 500 e mais
//   que 1,5× o preço de venda da fórmula inteira — em manipulação isso não existe; são
//   requisições de teste/canceladas ou unidade trocada (ex.: PANCREATINA 250.000 g,
//   2 × R$ 77,5 mil em abr/26; VITAMINA D 5UG em UI). Excluídas do custo, listadas à parte.
// · Perda de verdade = ajuste de BAIXA com causa 'descarte por validade' ou 'perda sem
//   causa identificada' (mart.estoque_ajuste). Recontagem/quebra/digitação não é perda.
// ============================================================
const SQL_LINHA_CUSTO = `
  fv AS (SELECT cdfil, nrrqu, serier, sum(prcobr) preco FROM mart.f_venda GROUP BY 1,2,3),
  lc AS (SELECT c.cdpro, c.nrrqu, c.dtentr, c.unida,
                (c.qtreal::numeric + coalesce(nullif(c.qtperda::text,'')::numeric,0)) q,
                c.prcusto::numeric custo,
                (c.prcusto::numeric > 500 AND c.prcusto::numeric > 1.5*greatest(coalesce(fv.preco,0),1)) suspeita
           FROM stg.venda_componente c
           LEFT JOIN fv ON fv.cdfil=c.cdfil AND fv.nrrqu=c.nrrqu AND fv.serier=c.serier)`;

async function blocoEstoque(){
  const [skus, resumo, parados, lotes, mensal, snap, topCusto, susp, forn] = await Promise.all([
    // itens COM giro — base do estoque ótimo e da fila de compra
    mart(`WITH sem AS (
        SELECT cdpro, date_trunc('week', dtentr)::date wk,
               sum(qtreal::numeric + coalesce(nullif(qtperda::text,'')::numeric,0)) q
          FROM stg.venda_componente WHERE dtentr >= current_date - 91 GROUP BY 1,2),
      wks AS (SELECT generate_series(date_trunc('week', current_date - 91)::date,
                                     date_trunc('week', current_date)::date - 7, interval '7 day')::date wk),
      var AS (SELECT g.cdpro, stddev_samp(coalesce(s.q,0)) sd_sem
                FROM (SELECT DISTINCT cdpro FROM sem) g CROSS JOIN wks w
                LEFT JOIN sem s ON s.cdpro=g.cdpro AND s.wk=w.wk GROUP BY 1),
      comp AS (SELECT cdpro, max(dtent)::date ult_compra,
                      (array_agg(fornecid ORDER BY dtent DESC))[1] ult_forn,
                      (array_agg(prunir::numeric ORDER BY dtent DESC))[1] ult_preco,
                      avg((dtval::date - dtent::date)::numeric) FILTER (WHERE dtval > dtent) vida,
                      count(DISTINCT nrnot) FILTER (WHERE dtent >= current_date - 365) compras12
                 FROM mart.compra GROUP BY 1),
      said AS (SELECT cdpro, max(dtentr)::date ult_saida FROM stg.venda_componente GROUP BY 1)
      SELECT s.cdpro, s.produto, s.unida, s.curva, s.grupo_nome grupo, s.perecivel, s.situacao, s.prioridade, s.motivo,
             round(s.saldo::numeric,3) saldo, round(s.custo::numeric,4) custo, round(s.valor::numeric,2) valor,
             round(s.consumo_mes::numeric/30.0,4) cdia, round(coalesce(v.sd_sem,0)::numeric/sqrt(7)::numeric,4) sddia,
             s.dias_cobertura cob, round(s.minimo::numeric,3) minimo, round(s.maximo::numeric,3) maximo,
             round(s.valor_vencido::numeric,2) vencido, round(s.valor_risco::numeric,2) risco,
             c.ult_compra, c.ult_forn, round(coalesce(c.ult_preco, s.custo::numeric),4) preco,
             round(c.vida) vida, coalesce(c.compras12,0) compras12, d.ult_saida
        FROM mart.estoque_sku s
        LEFT JOIN var v ON v.cdpro=s.cdpro LEFT JOIN comp c ON c.cdpro=s.cdpro LEFT JOIN said d ON d.cdpro=s.cdpro
       WHERE s.consumo_mes > 0`),
    mart(`SELECT situacao, prioridade, grupo_nome grupo, count(*) skus, round(sum(valor)::numeric,2) valor,
                 round(sum(valor_vencido)::numeric,2) vencido, round(sum(valor_risco)::numeric,2) risco
            FROM mart.estoque_sku GROUP BY 1,2,3`),
    // parados: sem saída em 90 dias e com saldo — capital imobilizado
    mart(`SELECT s.cdpro, s.produto, s.unida, s.curva, s.grupo_nome grupo, round(s.saldo::numeric,3) saldo,
                 round(s.valor::numeric,2) valor, round(s.valor_vencido::numeric,2) vencido, s.proxima_validade::date validade,
                 d.ult_saida, c.ult_compra
            FROM mart.estoque_sku s
            LEFT JOIN (SELECT cdpro, max(dtentr)::date ult_saida FROM stg.venda_componente GROUP BY 1) d ON d.cdpro=s.cdpro
            LEFT JOIN (SELECT cdpro, max(dtent)::date ult_compra FROM mart.compra GROUP BY 1) c ON c.cdpro=s.cdpro
           WHERE s.situacao='Parado' AND s.valor > 0 ORDER BY s.valor DESC LIMIT 400`),
    // lotes: vencidos e em risco — só perecível (M/R/D) e sem lote fantasma
    mart(`SELECT cdpro, produto, grupo_nome grupo, nrlot, dtval::date validade, dias, unida,
                 round(saldo::numeric,3) saldo, round(valor::numeric,2) valor, round(valor_em_risco::numeric,2) risco,
                 round(qt_vence_antes_girar::numeric,3) qt_risco, situacao_validade
            FROM mart.estoque_lote
           WHERE perecivel AND NOT fantasma AND saldo > 0 AND (dias < 0 OR valor_em_risco > 0 OR dias <= 90)
           ORDER BY valor DESC LIMIT 600`),
    // série mensal: custo real consumido × compras × perdas (balanço do Thome)
    mart(`WITH ${SQL_LINHA_CUSTO},
      cons AS (SELECT to_char(dtentr,'YYYY-MM') mes, round(sum(custo) FILTER (WHERE NOT suspeita)) custo,
                      round(sum(custo) FILTER (WHERE suspeita)) suspeito FROM lc GROUP BY 1),
      comp AS (SELECT to_char(dtent,'YYYY-MM') mes, round(sum(valor)) compras, count(DISTINCT nrnot) notas FROM mart.compra GROUP BY 1),
      aj AS (SELECT to_char(datarf,'YYYY-MM') mes,
                    round(sum(valor) FILTER (WHERE tipo='baixa' AND causa='descarte por validade')) validade,
                    round(sum(valor) FILTER (WHERE tipo='baixa' AND causa='perda sem causa identificada')) sem_causa,
                    round(sum(valor) FILTER (WHERE tipo='baixa' AND causa NOT IN ('descarte por validade','perda sem causa identificada'))) outras
               FROM mart.estoque_ajuste GROUP BY 1)
      SELECT m.mes, coalesce(cons.custo,0) custo, coalesce(cons.suspeito,0) suspeito, coalesce(comp.compras,0) compras,
             coalesce(comp.notas,0) notas, coalesce(aj.validade,0) validade, coalesce(aj.sem_causa,0) sem_causa, coalesce(aj.outras,0) outras
        FROM (SELECT mes FROM cons UNION SELECT mes FROM comp) m
        LEFT JOIN cons ON cons.mes=m.mes LEFT JOIN comp ON comp.mes=m.mes LEFT JOIN aj ON aj.mes=m.mes
       WHERE m.mes >= '2025-01' AND m.mes <= to_char(current_date,'YYYY-MM') ORDER BY 1`),
    mart(`SELECT to_char(data,'YYYY-MM-DD') data, round(sum(valor)) valor,
                 round(sum(valor) FILTER (WHERE situacao IN ('Excesso','Parado'))) parado,
                 round(sum(valor_vencido)) vencido, count(*) FILTER (WHERE situacao='Ruptura') ruptura
            FROM mart.estoque_snapshot GROUP BY 1 ORDER BY 1`),
    // onde está o custo: insumos que mais pesam em 12 meses + preço pago antes × agora
    mart(`WITH ${SQL_LINHA_CUSTO},
      c12 AS (SELECT cdpro, sum(custo) custo12, sum(q) q12 FROM lc WHERE NOT suspeita AND dtentr >= current_date - 365 GROUP BY 1),
      pr AS (SELECT cdpro,
                    sum(prunir::numeric*qtreal::numeric) FILTER (WHERE dtent <  current_date-180) / nullif(sum(qtreal::numeric) FILTER (WHERE dtent <  current_date-180),0) antes,
                    sum(prunir::numeric*qtreal::numeric) FILTER (WHERE dtent >= current_date-180) / nullif(sum(qtreal::numeric) FILTER (WHERE dtent >= current_date-180),0) agora
               FROM mart.compra WHERE dtent >= current_date - 365 GROUP BY 1)
      SELECT c12.cdpro, coalesce(p.descrprd,'(sem cadastro)') produto, p.unida, round(c12.custo12) custo12, round(c12.q12,1) q12,
             round(pr.antes,4) preco_antes, round(pr.agora,4) preco_agora
        FROM c12 LEFT JOIN stg.dim_produto p ON p.cdpro=c12.cdpro LEFT JOIN pr ON pr.cdpro=c12.cdpro
       ORDER BY c12.custo12 DESC LIMIT 30`),
    mart(`WITH ${SQL_LINHA_CUSTO}
      SELECT to_char(lc.dtentr,'YYYY-MM-DD') data, lc.nrrqu, lc.cdpro, coalesce(p.descrprd,'(sem cadastro)') produto,
             round(lc.q,2) q, lc.unida, round(lc.custo) custo
        FROM lc LEFT JOIN stg.dim_produto p ON p.cdpro=lc.cdpro WHERE lc.suspeita ORDER BY lc.custo DESC LIMIT 80`),
    // compras por fornecedor em 12 meses (nome vem de stg.dim_fornecedor, mais abaixo)
    mart(`SELECT fornecid, round(sum(valor)) valor, count(DISTINCT nrnot) notas, count(DISTINCT cdpro) itens, max(dtent)::date ultima
            FROM mart.compra WHERE dtent >= current_date - 365 GROUP BY 1 ORDER BY 2 DESC LIMIT 25`),
  ]);
  const num = r => { for(const k in r) if(typeof r[k]==='string' && /^-?\d+(\.\d+)?$/.test(r[k]) && !['cdpro','nrlot','fornecid','nrrqu'].includes(k)) r[k]=Number(r[k]); return r; };
  const d = x => x ? String(x).slice(0,10) : null;
  // Fornecedor (stg.dim_fornecedor = FC02000) e duplicatas das compras (stg.compra_duplicata = FC11200).
  // Tolerante: se o ETL ainda não criou as tabelas, o portal segue mostrando só o código.
  let fornNomes = {}, pagar = [];
  try{
    const [nomes, venc, prazo] = await Promise.all([
      mart(`SELECT f.fornecid, coalesce(nullif(f.fanta,''), f.razao) nome, f.munic cidade, f.unfed uf,
                   f.diasprazo, f.vrminfat
              FROM stg.dim_fornecedor f WHERE f.fornecid IN (SELECT DISTINCT fornecid FROM mart.compra)`),
      // o que ainda vence das compras já feitas (DTDUP de hoje em diante), por mês
      mart(`SELECT to_char(dtdup,'YYYY-MM') mes, round(sum(vrdup)) valor, count(*) parcelas
              FROM stg.compra_duplicata WHERE dtdup >= current_date AND dtdup < current_date + 365 GROUP BY 1 ORDER BY 1`),
      // prazo REAL de pagamento: dias entre a entrada da nota e o vencimento, ponderado pelo valor (12 meses)
      mart(`SELECT fornecid, round(sum((dtdup - dtent) * vrdup) / nullif(sum(vrdup),0)) dias, count(DISTINCT nrnot) notas
              FROM stg.compra_duplicata WHERE dtent >= current_date - 365 AND dtdup BETWEEN dtent AND dtent + 365
             GROUP BY 1`),
    ]);
    nomes.forEach(r=>{ fornNomes[r.fornecid] = { nome:r.nome, cidade:r.cidade, uf:r.uf, prazo:N(r.diasprazo)||null, minimo:N(r.vrminfat)||null }; });
    prazo.forEach(r=>{ (fornNomes[r.fornecid] ||= {}).prazo_real = N(r.dias); });
    pagar = venc.map(num);
  }catch(e){ console.warn('[estoque] fornecedor/duplicata indisponível —', (e.message||'').slice(0,90)); }
  return {
    fornNomes, pagar,
    ref: new Date().toISOString().slice(0,10),
    skus: skus.map(num).map(r=>({...r, ult_compra:d(r.ult_compra), ult_saida:d(r.ult_saida)})),
    resumo: resumo.map(num), parados: parados.map(num).map(r=>({...r, validade:d(r.validade), ult_saida:d(r.ult_saida), ult_compra:d(r.ult_compra)})),
    lotes: lotes.map(num).map(r=>({...r, validade:d(r.validade)})), mensal: mensal.map(num), snapshot: snap.map(num),
    topCusto: topCusto.map(num), suspeitos: susp.map(num), fornecedores: forn.map(num).map(r=>({...r, ultima:d(r.ultima)})),
  };
}

// ESTOQUE ITEM — ficha de um produto: ?cdpro=NNN
async function blocoEstoqueItem(params){
  const cd = Number(params && params.get ? params.get('cdpro') : NaN);
  if(!Number.isFinite(cd)) throw new Error('cdpro inválido');
  const [sku, lotes, consumo, compras, ajustes] = await Promise.all([
    mart(`SELECT * FROM mart.estoque_sku WHERE cdpro=${cd}`),
    mart(`SELECT nrlot, dtval::date validade, dias, round(saldo::numeric,3) saldo, round(valor::numeric,2) valor, fantasma,
                 situacao_validade, round(valor_em_risco::numeric,2) risco, nrnot, fornecid
            FROM mart.estoque_lote WHERE cdpro=${cd} ORDER BY dtval NULLS LAST`),
    mart(`SELECT to_char(dtentr,'YYYY-MM') mes, round(sum(qtreal::numeric + coalesce(nullif(qtperda::text,'')::numeric,0)),3) q,
                 round(sum(prcusto::numeric)) custo, count(*) linhas
            FROM stg.venda_componente WHERE cdpro=${cd} AND dtentr >= date '2025-01-01' GROUP BY 1 ORDER BY 1`),
    mart(`SELECT dtent::date data, nrnot, fornecid, round(qtreal::numeric,3) qt, unida, round(prunir::numeric,4) preco,
                 round(valor::numeric,2) valor, dtval::date validade
            FROM mart.compra WHERE cdpro=${cd} ORDER BY dtent DESC LIMIT 40`),
    mart(`SELECT datarf::date data, tipo, causa, round(quantdiferenca::numeric,3) qt, round(valor::numeric,2) valor, nrlot
            FROM mart.estoque_ajuste WHERE cdpro=${cd} ORDER BY datarf DESC LIMIT 40`),
  ]);
  const d = x => x ? String(x).slice(0,10) : null;
  return { sku: sku[0] || null,
           lotes: lotes.map(r=>({...r, validade:d(r.validade)})), consumo,
           compras: compras.map(r=>({...r, data:d(r.data), validade:d(r.validade)})),
           ajustes: ajustes.map(r=>({...r, data:d(r.data)})) };
}

// ============================================================
// ATENDIMENTO — tela "Hoje" do atendimento no Kommo (WhatsApp). Lê o Kommo AO VIVO
// (/api/v4/events), sem tabela nova: a resposta fica em cache por 2 min e, depois disso,
// quem abre recebe o cache na hora enquanto a API busca o novo por trás.
// Só lê metadado (quem, quando, qual lead) — o texto das mensagens nunca é buscado.
//
// Regras (1ª versão, a calibrar com o João):
// · "esperando resposta" = lead aberto cuja ÚLTIMA mensagem do cliente é mais nova que a
//   última resposta de uma PESSOA da equipe. Robô (created_by 0) não conta como resposta.
//   Janela: mensagens desde ontem 00:00. Tempo de espera em relógio corrido.
// · ATRASADO = esperando há mais de 1 hora.
// · 1ª resposta = da 1ª mensagem do cliente hoje até a 1ª resposta humana depois dela.
// · Leads que chegaram hoje = NOVOS (criados hoje) + REATIVADOS (lead antigo cujo cliente não
//   escrevia havia 7 dias ou mais e voltou a escrever hoje).
// Credenciais: ../sinequa-kommo/kommo.env (KOMMO_BASE, KOMMO_TOKEN) — o mesmo da recompra.
// ============================================================
carregaEnv(path.join(__dirname,'..','sinequa-kommo','kommo.env'));
const KOMMO_TOKEN = (process.env.KOMMO_TOKEN||'').trim();
let KOMMO_BASE = (process.env.KOMMO_BASE||'').trim();
try{ KOMMO_BASE = new URL(KOMMO_BASE).origin; }catch{ KOMMO_BASE = KOMMO_BASE.replace(/\/+$/,''); }
const _sleep = ms => new Promise(r=>setTimeout(r,ms));
async function kommo(p, tentativa=1){
  let r;
  try{ r = await fetch(KOMMO_BASE+p,{ headers:{ Authorization:'Bearer '+KOMMO_TOKEN, Accept:'application/json' }, signal: AbortSignal.timeout(20000) }); }
  catch(e){ if(tentativa<3){ await _sleep(800*tentativa); return kommo(p, tentativa+1); } throw e; }   // queda de rede: tenta de novo
  if(r.status===429 && tentativa<3){ await _sleep(1500*tentativa); return kommo(p, tentativa+1); }       // limite de requisições do Kommo
  if(r.status===204) return null;
  if(!r.ok) throw new Error('kommo '+r.status+' '+p.split('?')[0]);
  return r.json();
}
async function kommoEventos(tipo, desde, ate){
  const out=[], fim = ate ? `&filter[created_at][to]=${ate-1}` : '';
  for(let pg=1; pg<=60; pg++){
    const j = await kommo(`/api/v4/events?filter[type][]=${tipo}&filter[created_at][from]=${desde}${fim}&limit=100&page=${pg}`);
    const e = j?._embedded?.events || []; out.push(...e);
    if(!j?._links?.next || !e.length) break;
    await _sleep(120);
  }
  return out;
}
// pipelines e usuários mudam pouco: cache de 1 h
let _kCad = null, _kCadTs = 0;
async function kommoCadastros(){
  if(_kCad && Date.now()-_kCadTs < 3600e3) return _kCad;
  const [pp, us] = await Promise.all([ kommo('/api/v4/leads/pipelines'), kommo('/api/v4/users?limit=250') ]);
  const etapas = {}, funis = {};
  for(const p of pp?._embedded?.pipelines || []){
    funis[p.id] = p.name;
    for(const s of p._embedded?.statuses || []) etapas[s.id] = { nome:s.name, ordem:s.sort, funil:p.id };
  }
  const usuarios = {}; for(const u of us?._embedded?.users || []) usuarios[u.id] = u.name;
  _kCad = { etapas, funis, usuarios }; _kCadTs = Date.now();
  return _kCad;
}
async function kommoLeads(ids){
  const out = {};
  for(let i=0; i<ids.length; i+=50){
    const q = ids.slice(i,i+50).map(id=>`filter[id][]=${id}`).join('&');
    const j = await kommo(`/api/v4/leads?${q}&limit=250`);
    for(const l of j?._embedded?.leads || []) out[l.id] = l;
  }
  return out;
}
// Quem o cliente procurou nos 7 dias ANTES de hoje: lead → última msg dele. Muda 1x por dia, então
// é buscado uma vez por dia e guardado (~20 páginas). Serve para separar o lead antigo que voltou a falar.
const DIAS_REATIVA = 7;
let _hist = null;
async function kommoHistorico(hoje){
  if(_hist && _hist.hoje===hoje) return _hist.ult;
  const ev = await kommoEventos('incoming_chat_message', hoje-DIAS_REATIVA*86400, hoje);
  const ult = {}; for(const e of ev) if(e.entity_type==='lead' && !(ult[e.entity_id]>=e.created_at)) ult[e.entity_id]=e.created_at;
  _hist = { hoje, ult };
  return ult;
}
// meia-noite de hoje em São Paulo (epoch s); o Brasil não tem horário de verão desde 2019
function _meiaNoiteSP(){ const d = new Date().toLocaleDateString('en-CA',{timeZone:'America/Sao_Paulo'}); return Math.floor(new Date(d+'T00:00:00-03:00').getTime()/1000); }
const _horaSP = s => +new Date(s*1000).toLocaleString('en-US',{timeZone:'America/Sao_Paulo',hour:'2-digit',hour12:false}) % 24;
const _mediana = a => { if(!a.length) return null; const s=[...a].sort((x,y)=>x-y); return s[Math.floor(s.length/2)]; };

async function montaAtendimento(){
  if(!KOMMO_TOKEN || !KOMMO_BASE) throw new Error('Kommo não configurado (kommo.env)');
  const agora = Math.floor(Date.now()/1000), hoje = _meiaNoiteSP(), ontem = hoje-86400, base28 = hoje-28*86400;
  const [ent, sai, novos, cad, erp, hist] = await Promise.all([
    kommoEventos('incoming_chat_message', ontem),
    kommoEventos('outgoing_chat_message', ontem),
    kommoEventos('lead_added', base28),
    kommoCadastros(),
    Promise.all([
      mart(`SELECT round(orcado) orcado FROM mart.diario WHERE data=current_date`),
      mart(`SELECT count(DISTINCT nrorc) n FROM mart.f_orcamento WHERE dtentr::date=current_date`),
    ]).catch(e=>{ console.warn('[atendimento] ERP indisponível —', e.message.slice(0,80)); return null; }),
    kommoHistorico(hoje),
  ]);
  // ---- por lead: última msg do cliente, última resposta humana, 1ª msg de hoje e 1ª resposta depois dela
  const L = {};
  const lead = id => (L[id] ||= { inUlt:0, outUlt:0, outQuem:null, inPrim:null, inHoje:0, outHoje:0 });
  for(const e of ent){ if(e.entity_type!=='lead') continue; const x=lead(e.entity_id);
    if(e.created_at>x.inUlt) x.inUlt=e.created_at;
    if(e.created_at>=hoje){ x.inHoje++; if(!x.inPrim || e.created_at<x.inPrim) x.inPrim=e.created_at; } }
  const humanas = sai.filter(e=>e.entity_type==='lead' && e.created_by);
  for(const e of humanas){ const x=lead(e.entity_id);
    if(e.created_at>x.outUlt){ x.outUlt=e.created_at; x.outQuem=e.created_by; }
    if(e.created_at>=hoje) x.outHoje++; }
  // 1ª resposta humana de hoje: primeira saída humana depois da 1ª entrada de hoje
  const tResp = [];
  for(const [id,x] of Object.entries(L)){
    if(!x.inPrim) continue;
    let r=null; for(const e of humanas) if(e.entity_id==id && e.created_at>=x.inPrim && (!r || e.created_at<r)) r=e.created_at;
    if(r) tResp.push((r-x.inPrim)/60);
  }
  // ---- fila: esperando resposta (busca etapa/nome só desses leads + os criados hoje)
  const esperaIds = Object.entries(L).filter(([,x])=>x.inUlt>x.outUlt).map(([id])=>+id);
  const novosHoje = novos.filter(e=>e.created_at>=hoje && e.entity_type==='lead');
  const idsHoje = [...new Set(novosHoje.map(e=>e.entity_id))];
  // Quem procurou hoje: NOVO (lead criado hoje) · REATIVADO (lead antigo, cliente sem falar nos 7 dias antes
  // de hoje — inclui quem responde à recompra) · EM ANDAMENTO (lead antigo que já vinha conversando).
  const falouHoje = Object.entries(L).filter(([,x])=>x.inHoje).map(([id])=>+id);
  const setHoje = new Set(idsHoje);
  const reativIds = falouHoje.filter(id=>!setHoje.has(id) && !hist[id]);
  const andamento = falouHoje.filter(id=>!setHoje.has(id) && hist[id]).length;
  const setReat = new Set(reativIds);
  const det = await kommoLeads([...new Set([...esperaIds, ...idsHoje, ...reativIds])]);
  const FECHADO = new Set([142,143]);   // 142 = ganho, 143 = perdido (fixo no Kommo)
  const etapa = l => l ? (cad.etapas[l.status_id]?.nome || (l.status_id===142?'Ganho':l.status_id===143?'Perdido':'—')) : '—';
  const fila = esperaIds.map(id=>{ const x=L[id], l=det[id];
      return { id, nome: l?.name || null, etapa: etapa(l), funil: l ? cad.funis[l.pipeline_id]||null : null,
               desde: x.inUlt, espera_min: Math.round((agora-x.inUlt)/60),
               ultimo: x.outQuem ? cad.usuarios[x.outQuem]||null : null, valor: l?.price||0, novo: setHoje.has(id), reativado: setReat.has(id),
               fechado: l ? FECHADO.has(l.status_id) : false }; })
    .filter(f=>!f.fechado).sort((a,b)=>b.espera_min-a.espera_min);
  // ---- leads por hora: hoje × média dos 4 últimos mesmos dias da semana
  const dowHoje = new Date(hoje*1000).getUTCDay();   // meia-noite SP = 03:00 UTC → mesmo dia
  const porHora = Array.from({length:24},()=>({hoje:0, reat:0, media:0}));
  novosHoje.forEach(e=>porHora[_horaSP(e.created_at)].hoje++);
  reativIds.forEach(id=>porHora[_horaSP(L[id].inPrim)].reat++);   // reativado: hora da 1ª msg do cliente hoje
  let diasRef = 0, totRef = 0; const vistos = new Set();
  for(let k=1;k<=4;k++){ const ini=hoje-7*k*86400, fim=ini+86400; vistos.add(ini);
    const dia = novos.filter(e=>e.entity_type==='lead' && e.created_at>=ini && e.created_at<fim);
    dia.forEach(e=>porHora[_horaSP(e.created_at)].media+=1/4); totRef+=dia.length; diasRef++; }
  // até esta hora nos mesmos dias (para comparar o parcial de hoje com o parcial deles)
  // a hora corrente entra proporcional aos minutos já passados (às 15h25, 25/60 da média das 15h)
  const hAgora = _horaSP(agora), fracHora = ((agora-hoje)%3600)/3600; let ateAgoraRef = 0;
  for(let h=0; h<hAgora; h++) ateAgoraRef += porHora[h].media;
  ateAgoraRef += porHora[hAgora].media*fracHora;
  const msgHora = Array.from({length:24},()=>({entrou:0, saiu:0}));
  ent.filter(e=>e.created_at>=hoje).forEach(e=>msgHora[_horaSP(e.created_at)].entrou++);
  humanas.filter(e=>e.created_at>=hoje).forEach(e=>msgHora[_horaSP(e.created_at)].saiu++);
  // ---- onde os leads de hoje estão agora
  const porEtapa = {};
  [...idsHoje, ...reativIds].forEach(id=>{ const l=det[id]; const nome=etapa(l); const o=(porEtapa[nome] ||= {etapa:nome, ordem: l? (cad.etapas[l.status_id]?.ordem ?? (l.status_id===142?9e3:9e3+1)) : 1e4, n:0, reat:0, valor:0}); o.n++; if(setReat.has(id)) o.reat++; o.valor+=l?.price||0; });
  const espera = fila.length, atrasado = fila.filter(f=>f.espera_min>60).length;
  const ultimoEvento = Math.max(0, ...ent.map(e=>e.created_at), ...sai.map(e=>e.created_at));
  return {
    gerado: new Date().toISOString(), kommo: KOMMO_BASE,
    ultimo_evento_min: ultimoEvento ? Math.round((agora-ultimoEvento)/60) : null,
    leads: { hoje: idsHoje.length, reativados: reativIds.length, andamento, dias_reativa: DIAS_REATIVA, media_dia: diasRef? Math.round(totRef/diasRef*10)/10 : null,
             media_ate_agora: Math.round(ateAgoraRef*10)/10, hora_agora: hAgora, dow: dowHoje },
    espera: { total: espera, atrasado },
    resposta: { mediana_min: _mediana(tResp)!=null ? Math.round(_mediana(tResp)) : null,
                ate15: tResp.length ? Math.round(tResp.filter(m=>m<=15).length/tResp.length*1000)/1000 : null,
                n: tResp.length },
    mensagens: { recebidas: ent.filter(e=>e.created_at>=hoje).length,
                 humanas: humanas.filter(e=>e.created_at>=hoje).length,
                 robo: sai.filter(e=>e.created_at>=hoje && !e.created_by).length,
                 conversas: Object.values(L).filter(x=>x.inHoje).length },
    erp: erp ? { orcado: N(erp[0][0]?.orcado), n: N(erp[1][0]?.n) } : null,
    porHora: porHora.map((h,i)=>({h:i, hoje:h.hoje, reat:h.reat, media:Math.round(h.media*10)/10})),
    msgHora: msgHora.map((h,i)=>({h:i, ...h})),
    etapas: Object.values(porEtapa).sort((a,b)=>a.ordem-b.ordem).map(({etapa,n,reat,valor})=>({etapa,n,reat,valor})),
    fila,
  };
}
let _atd = null, _atdTs = 0, _atdBusca = null;
function _buscaAtd(){
  if(!_atdBusca) _atdBusca = montaAtendimento().then(d=>{ _atd=d; _atdTs=Date.now(); return d; }).finally(()=>{ _atdBusca=null; });
  return _atdBusca;
}
async function blocoAtendimento(){
  const idade = Date.now()-_atdTs;
  if(_atd && idade < 120e3) return _atd;
  if(_atd && idade < 15*60e3){ _buscaAtd().catch(e=>console.error('[atendimento]', e.message)); return _atd; }  // entrega o anterior e atualiza por trás
  return _buscaAtd();
}

const BLOCOS = { ano: blocoAno, mes: blocoMes, dia: blocoDia, producao: blocoProd, busca: blocoBusca,
                 estoque: blocoEstoque, estoqueitem: blocoEstoqueItem, atendimento: blocoAtendimento };

// ---------- Chatbot (Nível 1): responde sobre o CONTEXTO do painel via Claude ----------
const IA_SYS = `Você é o assistente do Portal Sinequa, o painel de gestão da Sinequa Farma (farmácia de manipulação em São Paulo). Responde perguntas do dono/gestor sobre os números do painel.

Regras:
- Português, direto e objetivo, como um analista de confiança da casa.
- Use SOMENTE os dados do CONTEXTO (JSON) abaixo. Se a resposta não estiver lá, diga que esse dado não está no painel — NUNCA invente número.
- Reais no formato R$ 217.129; percentuais com 1 casa (43,3%).
- Conciso: 1 a 4 frases, ou uma lista curta. Sem enrolação, sem repetir a pergunta.
- Glossário: "ativo" = fórmula/princípio ativo; "meta"/"super" = metas do mês; "conversão" = venda/orçado; "recência" = dias desde a última compra.`;

function lerCorpo(req){ return new Promise((res,rej)=>{ let d=''; req.on('data',c=>{ d+=c; if(d.length>2e6) req.destroy(); }); req.on('end',()=>res(d)); req.on('error',rej); }); }

async function chat(messages, contexto){
  if(!IA_KEY) throw new Error('IA não configurada');
  const sys = IA_SYS + '\n\nCONTEXTO (JSON):\n' + JSON.stringify(contexto);
  const r = await fetch('https://api.anthropic.com/v1/messages',{ method:'POST',
    headers:{ 'x-api-key':IA_KEY, 'anthropic-version':'2023-06-01', 'content-type':'application/json' },
    body: JSON.stringify({ model:IA_MODEL, max_tokens:800, system:sys, messages }),
    signal: AbortSignal.timeout(30000) });
  const j = await r.json();
  if(!r.ok) throw new Error((j&&j.error&&j.error.message)||('HTTP '+r.status));
  return (j.content||[]).filter(b=>b.type==='text').map(b=>b.text).join('').trim() || '(sem resposta)';
}

// ---------- FILA DE ORÇAMENTOS (pedidos dos grupos de WhatsApp) ----------
// A fila mora no serviço do WhatsApp (sinequa-whatsapp/recebe-whatsapp.js, 127.0.0.1:8095), que tem o banco, as
// receitas no disco e o estado de quem assumiu. Aqui só confere a chave e repassa:
//   GET /portal/fila  ·  POST /portal/fila/acao  ·  GET /portal/midia/<id>?key=  (a imagem vai num <img>, por isso aceita ?key=)
const WHATS = (process.env.WHATS_URL || 'http://127.0.0.1:8095').replace(/\/$/,'');
// Senha dos orçamentistas: FILA_SENHA (env) ou a linha FILA_SENHA do whatsapp.env (lê SÓ essa linha — o resto daquele
// arquivo tem PGHOST etc., que mudariam o backend desta API). Quem acerta a senha recebe a chave do portal.
function filaSenha(){
  if(process.env.FILA_SENHA) return process.env.FILA_SENHA;
  try{ const m = fs.readFileSync(path.join(__dirname,'..','sinequa-whatsapp','whatsapp.env'),'utf8').match(/^\s*FILA_SENHA\s*=\s*(.*?)\s*$/m); return m ? m[1] : ''; }catch{ return ''; }
}
const _tentativas = new Map();   // ip → [instantes] — freia chute de senha (10 por 10 min)
async function repassa(req, res, origem, destino){
  try{
    const corpo = req.method==='POST' ? await lerCorpo(req) : undefined;
    const r = await fetch(WHATS+destino, { method:req.method, headers: corpo ? {'content-type':'application/json'} : {}, body:corpo, signal:AbortSignal.timeout(30000) });
    const h = { 'Content-Type': r.headers.get('content-type')||'application/json', 'Cache-Control': r.headers.get('cache-control')||'no-store' };
    if(r.headers.get('content-disposition')) h['Content-Disposition'] = r.headers.get('content-disposition');
    if(ORIGENS.includes('*')) h['Access-Control-Allow-Origin']='*'; else if(origem && ORIGENS.includes(origem)){ h['Access-Control-Allow-Origin']=origem; h['Vary']='Origin'; }
    res.writeHead(r.status, h); res.end(Buffer.from(await r.arrayBuffer()));
  }catch(e){
    console.error('[fila]', e.message);
    responde(res, origem, 503, { erro:'O serviço do WhatsApp não respondeu. Ele roda no servidor pela tarefa Sinequa WhatsApp.' });
  }
}

// ---------- HTTP ----------
function responde(res, origem, status, corpo){
  const h = {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'};
  if(ORIGENS.includes('*')){ h['Access-Control-Allow-Origin']='*'; }
  else if(origem && ORIGENS.includes(origem)){ h['Access-Control-Allow-Origin']=origem; h['Vary']='Origin'; }
  res.writeHead(status,h); res.end(JSON.stringify(corpo));
}
const servidor = http.createServer(async (req,res)=>{
  const origem = req.headers.origin;
  if(req.method==='OPTIONS'){ res.writeHead(204,{'Access-Control-Allow-Origin':ORIGENS.includes('*')?'*':(ORIGENS.includes(origem)?origem:ORIGENS[0]||'*'),'Access-Control-Allow-Methods':'GET, POST, OPTIONS','Access-Control-Allow-Headers':'x-portal-key, content-type','Access-Control-Max-Age':'86400'}); return res.end(); }
  const url = new URL(req.url,'http://x');
  if(url.pathname==='/portal/saude') return responde(res,origem,200,{ok:true,hora:new Date().toISOString(),backend:BACKEND,fonte:USA_PG?(process.env.DATABASE_URL?'DATABASE_URL':(process.env.PGHOST||'localhost')+':'+(process.env.PGPORT||5432)+'/'+(process.env.PGDATABASE||'sinequa')):MB,ia:!!IA_KEY});
  // Chatbot — POST /portal/chat {messages:[{role,content}], contexto}
  if(req.method==='POST' && url.pathname==='/portal/chat'){
    const key = req.headers['x-portal-key'] || url.searchParams.get('key') || '';
    if(key!==TOKEN) return responde(res,origem,401,{erro:'não autorizado'});
    try{
      const body = JSON.parse((await lerCorpo(req)) || '{}');
      const messages = Array.isArray(body.messages) ? body.messages.filter(m=>m&&m.role&&m.content).slice(-12) : [];
      if(!messages.length) return responde(res,origem,400,{erro:'sem mensagem'});
      const resposta = await chat(messages, body.contexto||{});
      return responde(res,origem,200,{resposta});
    }catch(e){
      const semKey = /IA não configurada/.test(e.message||'');
      console.error('[chat]', e.message);
      return responde(res,origem, semKey?503:500, {erro: semKey?'O assistente ainda não está configurado no servidor.':'Não consegui responder agora.', detalhe:(e.message||'').slice(0,300)});
    }
  }
  if(req.method==='GET' && (url.pathname==='/fila' || url.pathname==='/fila/')) return repassa(req, res, origem, '/fila.html');   // tela dos orçamentistas
  if(req.method==='POST' && url.pathname==='/portal/fila/entrar'){
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?', agora = Date.now();
    const t = (_tentativas.get(ip) || []).filter(x => agora - x < 10*60e3);
    if(t.length >= 10) return responde(res, origem, 429, { erro:'Muitas tentativas. Espere alguns minutos.' });
    const senha = filaSenha();
    if(!senha) return responde(res, origem, 503, { erro:'A senha da fila ainda não foi configurada no servidor.' });
    let b = {}; try{ b = JSON.parse(await lerCorpo(req) || '{}'); }catch{}
    if(String(b.senha||'') !== senha){ t.push(agora); _tentativas.set(ip, t); return responde(res, origem, 401, { erro:'Senha incorreta.' }); }
    return responde(res, origem, 200, { chave: TOKEN });
  }
  if(url.pathname==='/portal/fila' || url.pathname==='/portal/fila/acao' || /^\/portal\/midia\/[A-Za-z0-9_-]+$/.test(url.pathname)){
    const key = req.headers['x-portal-key'] || url.searchParams.get('key') || '';
    if(key!==TOKEN) return responde(res,origem,401,{erro:'não autorizado'});
    return repassa(req, res, origem, url.pathname.replace(/^\/portal/,''));
  }
  const m = url.pathname.match(/^\/portal\/([a-z]+)$/);
  if(!m || !BLOCOS[m[1]]) return responde(res,origem,404,{erro:'rota desconhecida'});
  // auth — token compartilhado
  const key = req.headers['x-portal-key'] || url.searchParams.get('key') || '';
  if(key!==TOKEN) return responde(res,origem,401,{erro:'não autorizado'});
  try{
    const dados = await BLOCOS[m[1]](url.searchParams);
    return responde(res,origem,200,dados);
  }catch(e){
    console.error('[erro]',m[1],e.message);
    const ocupado = /timeout|ETIMEDOUT|canceling statement/i.test(e.message||'');
    return responde(res,origem,ocupado?503:500,{erro: ocupado?'Atualizando os dados, tente em segundos.':'Não foi possível consultar agora.'});
  }
});
servidor.listen(PORTA,()=>{
  const fonte = USA_PG ? (process.env.DATABASE_URL?'DATABASE_URL':`${process.env.PGHOST||'localhost'}:${process.env.PGPORT||5432}/${process.env.PGDATABASE||'sinequa'}`) : MB;
  console.log(`API do Portal em http://localhost:${PORTA}  (mart via ${BACKEND} → ${fonte})`);
  console.log(`  teste: curl -s "http://localhost:${PORTA}/portal/ano?key=${TOKEN}"`);
});
