// Extrai o modelo completo do Fórmula Certa (queries do BI) -> staging no Postgres.
// CARGA RESILIENTE:
//   1) puxa cada tabela p/ stg.<nome>__new (não toca nos dados vivos);
//   2) timeout + retry com reconexão na Firebird (contra conexão-zumbi p.ex. do Mac dormindo);
//   3) só se TODAS vierem OK, faz o swap transacional (TRUNCATE+INSERT, mantendo as views);
//   4) qualquer falha => ROLLBACK e os dados vivos seguem intactos (dashboards nunca quebram).
// Exit: 0 = ok; 10 = ok mas schema mudou (run-extract.sh recria mart); 1 = falha (sem troca).
const fs=require('fs'), path=require('path');
const Firebird=require('node-firebird');
const { Client }=require('pg');
for(const l of fs.readFileSync(path.join(__dirname,'fb.env'),'utf8').split('\n')){
  const m=l.match(/^\s*([A-Z_]+)\s*=\s*(.*?)\s*$/); if(m) process.env[m[1]]=m[2].trim();
}
const fbOpt={host:process.env.FB_HOST,port:+process.env.FB_PORT||3050,database:process.env.FB_DB,
  user:process.env.FB_USER,password:process.env.FB_PASSWORD,lowercase_keys:false,encoding:'WIN1252'};
const DESDE="'2025-01-01'";

const jobs=[
 { name:'venda_capa', sql:`SELECT CDFIL,NRRQU,NRCPMF,CDFILD,CDCLI,CDCON,CDFUN,CDCAPTACAO,CDMVTO,DTENTR,VRRQU,VRDSC,VRTXA,FLAGENV,CONDP FROM FC12000 WHERE DTENTR>=${DESDE}` },
 { name:'venda_formula', sql:`SELECT CDFIL,NRRQU,SERIER,NRORC,SERIEO,DTENTR,DTRET,DTPRESCR,CDCLI,NOMEPA,PFCRM,UFCRM,NRCRM,TITROT,POSOL,GRUPOTERAP,VOLUME,UNIVOL,TPCAP,QTFOR,QTPRESCR,QTCAPDIA,QTDIASTRAT,PRCOBR,PRREAL,PRCUSTO,VRDSC,PTDSC,VRTXA,FLAGDEV,INDCONFERIDO FROM FC12100 WHERE DTENTR>=${DESDE}` },
 { name:'venda_componente', sql:`WITH FAT AS (SELECT CDPRO, MAX(FATOR) AS FATOR FROM FC03000 WHERE INDDEL='N' GROUP BY CDPRO)
    SELECT c.CDFIL,c.NRRQU,c.SERIER,c.ITEMID,c.CDPRO,c.CDPRIN,c.DESCR,c.TPCMP,c.UNIDA,c.NRLOT,c.INDQSP,c.INDVEICULO,c.DTENTR,c.FLAGATU,
      CAST(c.QUANT AS VARCHAR(40)) QUANT, CAST(c.QTREAL AS VARCHAR(40)) QTREAL, CAST(c.QTPERDA AS VARCHAR(40)) QTPERDA,
      CAST(c.VRCMP AS VARCHAR(40)) VRCMP, CAST(c.PRCUSTO AS VARCHAR(40)) PRCUSTO, CAST(c.PRCOMPRA AS VARCHAR(40)) PRCOMPRA,
      CAST(COALESCE(fpro.FATOR,fprin.FATOR) AS DOUBLE PRECISION) FATOR_USADO,
      CAST((f.PRCOBR-f.VRDSC)*(c.PRCUSTO*COALESCE(fpro.FATOR,fprin.FATOR))/NULLIF(w.peso,0) AS DOUBLE PRECISION) PRECO_VENDA_COMP
    FROM FC12110 c JOIN FC12100 f ON f.CDFIL=c.CDFIL AND f.NRRQU=c.NRRQU AND f.SERIER=c.SERIER
    LEFT JOIN FAT fpro ON fpro.CDPRO=c.CDPRO LEFT JOIN FAT fprin ON fprin.CDPRO=c.CDPRIN
    JOIN (SELECT cc.CDFIL,cc.NRRQU,cc.SERIER,SUM(cc.PRCUSTO*COALESCE(fp.FATOR,fn.FATOR)) peso FROM FC12110 cc
          LEFT JOIN FAT fp ON fp.CDPRO=cc.CDPRO LEFT JOIN FAT fn ON fn.CDPRO=cc.CDPRIN GROUP BY cc.CDFIL,cc.NRRQU,cc.SERIER) w
      ON w.CDFIL=c.CDFIL AND w.NRRQU=c.NRRQU AND w.SERIER=c.SERIER
    WHERE f.DTENTR>=${DESDE}` },
 { name:'orcamento_capa', sql:`SELECT CDFIL,NRORC,CDFILD,CDCLI,CDCON,CDFUN,CDCAPTACAO,DTENTR,VRRQU,VRDSC,VRTXA,FLAGENV,CONDP,NOMEPA FROM FC15000 WHERE DTENTR>=${DESDE}` },
 { name:'orcamento_formula', sql:`SELECT CDFIL,NRORC,SERIEO,PFCRM,UFCRM,NRCRM,CDCLI,NOMEPA,TITROT,POSOL,GRUPOTERAP,VOLUME,UNIVOL,TPCAP,QTFOR,DTENTR,DTRET,DTPRESCR,PRCOBR,PRREAL,PRCUSTO,VRDSC,PTDSC,VRTXA FROM FC15100 WHERE DTENTR>=${DESDE}` },
 { name:'orcamento_componente', sql:`WITH FAT AS (SELECT CDPRO, MAX(FATOR) AS FATOR FROM FC03000 WHERE INDDEL='N' GROUP BY CDPRO)
    SELECT c.CDFIL,c.NRORC,c.SERIEO,c.ITEMID,c.CDPRO,c.CDPRIN,c.DESCR,c.TPCMP,c.UNIDA,c.INDQSP,c.INDVEICULO,c.DTENTR,
      CAST(c.QUANT AS VARCHAR(40)) QUANT, CAST(c.QTREAL AS VARCHAR(40)) QTREAL, CAST(c.QTPERDA AS VARCHAR(40)) QTPERDA,
      CAST(c.VRCMP AS VARCHAR(40)) VRCMP, CAST(c.PRCUSTO AS VARCHAR(40)) PRCUSTO, CAST(c.PRCOMPRA AS VARCHAR(40)) PRCOMPRA,
      CAST(COALESCE(fpro.FATOR,fprin.FATOR) AS DOUBLE PRECISION) FATOR_USADO,
      CAST((f.PRCOBR-f.VRDSC)*(c.PRCUSTO*COALESCE(fpro.FATOR,fprin.FATOR))/NULLIF(w.peso,0) AS DOUBLE PRECISION) PRECO_VENDA_COMP
    FROM FC15110 c JOIN FC15100 f ON f.CDFIL=c.CDFIL AND f.NRORC=c.NRORC AND f.SERIEO=c.SERIEO
    LEFT JOIN FAT fpro ON fpro.CDPRO=c.CDPRO LEFT JOIN FAT fprin ON fprin.CDPRO=c.CDPRIN
    JOIN (SELECT cc.CDFIL,cc.NRORC,cc.SERIEO,SUM(cc.PRCUSTO*COALESCE(fp.FATOR,fn.FATOR)) peso FROM FC15110 cc
          LEFT JOIN FAT fp ON fp.CDPRO=cc.CDPRO LEFT JOIN FAT fn ON fn.CDPRO=cc.CDPRIN GROUP BY cc.CDFIL,cc.NRORC,cc.SERIEO) w
      ON w.CDFIL=c.CDFIL AND w.NRORC=c.NRORC AND w.SERIEO=c.SERIEO
    WHERE f.DTENTR>=${DESDE}` },
 { name:'receber_capa', sql:`SELECT cx.CDFIL,cx.NRRQU,cx.CDCLI,cx.PFCRM,cx.UFCRM,cx.NRCRM,cx.CDCON,cx.CDFUN,cx.TPRQU,cx.DTENTR,cx.DTEFE,cx.DTVENC,cx.DTPAG,
      CAST(cx.VRRQU AS VARCHAR(40)) VRRQU, CAST(cx.VRRCB AS VARCHAR(40)) VRRCB, CAST(cx.VRSDO AS VARCHAR(40)) VRSDO,
      CAST(cx.VRLIQ AS VARCHAR(40)) VRLIQ, CAST(cx.VRDSC AS VARCHAR(40)) VRDSC, CAST(cx.VRTXA AS VARCHAR(40)) VRTXA,
      cx.NRNOT,cx.NRNFCE,cx.NRNFSAT,cx.NRNFSE,cx.FLAGDEV,cx.CDFILENTG,cx.NRENTG FROM FC17000 cx WHERE cx.DTENTR>=${DESDE}` },
 { name:'receber_parcela', sql:`SELECT cx.CDFIL,cx.NRRQU,cx.ITEMID,cx.TPRCB,cx.DTPAG,cx.DTOPE,
      CAST(cx.VRRCB AS VARCHAR(40)) VRRCB, CAST(cx.VRTXA AS VARCHAR(40)) VRTXA, CAST(cx.VRDSC AS VARCHAR(40)) VRDSC, cx.OBSERRCB FROM FC17100 cx WHERE cx.DTPAG>=${DESDE}` },
 { name:'receber_forma', sql:`SELECT CDFIL,NRRQU,FRMID,FMPAG,VRPAG,NPARCELA,CDADM,CDBAN,NRCHQ,DTOPE,DTDEP,CDCON FROM FC17110 WHERE DTOPE>=${DESDE}` },
 // FC07000 = cadastro do cliente/paciente; FC07200 = endereços (1 linha por ocorrência, chave
 // CDCLI+OCENDER). Pega a ocorrência do endereço de ENTREGA (OCENDERENTREGA), caindo p/ a 1.
 // RG = NRINSCR (número) + OERG (órgão) + UFRG (uf). CPF = NRCNPJ. Nascimento = DTNAS. Cadastro = DTCAD.
 // Telefone NÃO vem daqui: o Fórmula não mantém o número do paciente (fica no CRM/WhatsApp).
 { name:'dim_cliente', sql:`SELECT C.CDFIL,C.CDCLI,C.NOMECLI,C.TPCLI,C.TPSEX,C.DTNAS,C.ECIVIL,C.PROFIS,C.UFNAS,C.EMAIL,C.DTCAD,C.CLCLI,C.CDINDICACAO,C.CDCONV,C.NRCNPJ,C.TPDOC,
      C.NRINSCR,C.OERG,C.UFRG,
      E.ENDER,E.ENDNR,E.ENDCP,E.BAIRR,E.NRCEP,E.MUNIC,E.UNFED
    FROM FC07000 C
    LEFT JOIN FC07200 E ON E.CDCLI=C.CDCLI AND E.OCENDER=COALESCE(NULLIF(C.OCENDERENTREGA,0),1)` },
 { name:'dim_medico', sql:`SELECT PFCRM,UFCRM,NRCRM,NOMEMED,CURVA,PTPAR FROM FC04000 WHERE NRCRM IS NOT NULL` },
 { name:'dim_filial', sql:`SELECT CDFIL,DESCRFIL,GRUPOFIL FROM FC01000` },
 { name:'dim_produto', sql:`SELECT CDPRO,DESCRPRD,DESCR,PRINCIPIOATIVO,CDDCB,CDCAS,GRUPO,SETOR,FAMIL,DIVIS,LINHA,CLASSE,CLASSETERAP,CATEGORIA,CURVA,UNIDA,TPMEDICAMENTO,INDUSOCONT,SITUA,PRVEN,PRCOM,EAN,FATOR FROM FC03000 WHERE INDDEL='N'` },
 { name:'resumo_orcamento', sql:`WITH FAT AS (SELECT CDPRO,MAX(FATOR) FATOR FROM FC03000 WHERE INDDEL='N' GROUP BY CDPRO)
    SELECT c.CDFIL,c.NRORC,c.SERIEO,
      (SELECT FIRST 1 TRIM(c2.DESCR) FROM FC15110 c2 LEFT JOIN FAT fp ON fp.CDPRO=c2.CDPRO LEFT JOIN FAT fn ON fn.CDPRO=c2.CDPRIN
        WHERE c2.CDFIL=c.CDFIL AND c2.NRORC=c.NRORC AND c2.SERIEO=c.SERIEO AND c2.TPCMP='C'
        ORDER BY (c2.PRCUSTO*COALESCE(fp.FATOR,fn.FATOR)) DESC) ATIVO_PRINCIPAL,
      COUNT(*) QTD_ATIVOS
    FROM FC15110 c WHERE c.TPCMP='C' AND EXISTS(SELECT 1 FROM FC15100 ff WHERE ff.CDFIL=c.CDFIL AND ff.NRORC=c.NRORC AND ff.SERIEO=c.SERIEO AND ff.DTENTR>=${DESDE})
    GROUP BY c.CDFIL,c.NRORC,c.SERIEO` },
 // ativo principal DA VENDA (espelho do resumo_orcamento, mas em FC12110, chave cdfil/nrrqu/serier). Alimenta mart.vencimentos.
 { name:'resumo_venda', sql:`WITH FAT AS (SELECT CDPRO,MAX(FATOR) FATOR FROM FC03000 WHERE INDDEL='N' GROUP BY CDPRO)
    SELECT c.CDFIL,c.NRRQU,c.SERIER,
      (SELECT FIRST 1 TRIM(c2.DESCR) FROM FC12110 c2 LEFT JOIN FAT fp ON fp.CDPRO=c2.CDPRO LEFT JOIN FAT fn ON fn.CDPRO=c2.CDPRIN
        WHERE c2.CDFIL=c.CDFIL AND c2.NRRQU=c.NRRQU AND c2.SERIER=c.SERIER AND c2.TPCMP='C'
        ORDER BY (c2.PRCUSTO*COALESCE(fp.FATOR,fn.FATOR)) DESC) ATIVO_PRINCIPAL,
      COUNT(*) QTD_ATIVOS
    FROM FC12110 c WHERE c.TPCMP='C' AND EXISTS(SELECT 1 FROM FC12100 ff WHERE ff.CDFIL=c.CDFIL AND ff.NRRQU=c.NRRQU AND ff.SERIER=c.SERIER AND ff.DTENTR>=${DESDE})
    GROUP BY c.CDFIL,c.NRRQU,c.SERIER` },
 { name:'dim_paciente', sql:`SELECT DISTINCT TRIM(NOMEPA) PACIENTE FROM FC15100 WHERE NOMEPA IS NOT NULL AND TRIM(NOMEPA)<>''
    UNION SELECT DISTINCT TRIM(NOMEPA) FROM FC12100 WHERE NOMEPA IS NOT NULL AND TRIM(NOMEPA)<>''` },
 // romaneio (FC12400 capa / FC12410 itens) — alimenta a etapa "A caminho" do rastreio do cliente.
 // Antes vinha do extrator da Tracken (que sai de cena); agora e carga propria.
 // ENDER entrou para alimentar o preenchimento automatico do cadastro no Kommo:
 // o endereco da ULTIMA entrega e o que comprovadamente funcionou — o pacote
 // chegou la. Vale mais que o cadastro do cliente, que envelhece sem aviso.
 // CDCLIDES = cliente de destino (pode diferir do CDCLI que pagou).
 { name:'romaneio_capa', sql:`SELECT CDFILENTG,NRENTG,DTENTG,HRENTG,DTENTGEF,CDCLI,CDCLIDES,CDREG,CDFUNEN,VRTOT,
     CAST(ENDER AS VARCHAR(120)) ENDER,
     CAST(MUNIC AS VARCHAR(50)) MUNIC, CAST(UNFED AS VARCHAR(4)) UNFED, FLAGENTG
   FROM FC12400 WHERE DTENTG >= ${DESDE}` },
 // JOIN em vez de EXISTS: o EXISTS levava >3min na Firebird, o JOIN faz em 2s.
 // DISTINCT porque FC12410 traz uma linha por fórmula do pedido.
 { name:'romaneio_item', sql:`SELECT DISTINCT c.CDFILENTG, c.NRENTG, c.CDPRO AS NRRQU
   FROM FC12410 c
   JOIN FC12400 h ON h.NRENTG = c.NRENTG AND h.CDFILENTG = c.CDFILENTG
   WHERE c.TPITM='R' AND h.DTENTG >= ${DESDE}` },
 { name:'dim_operacao12530', sql:`SELECT TRIM(CDOPERA) COD_OPERACAO, TRIM(DESCRICAO) OPERACAO FROM FC12530` },
 { name:'dim_etapa12540', sql:`SELECT TRIM(CDETAPA) COD_ETAPA,TRIM(DESCRICAO) ETAPA,POSICAO ORDEM_ETAPA,TRIM(REFERENCIA) REFERENCIA,TRIM(OBRIGATORIA) OBRIGATORIA,TRIM(FINALIZA) FINALIZA,PRODUCAO IND_PRODUCAO,TRIM(TPPCP) TIPO_PCP FROM FC12540` },
 { name:'dim_motivo12550', sql:`SELECT TRIM(CDMOTIVO) COD_MOTIVO, TRIM(DESCRICAO) MOTIVO FROM FC12550` },
 { name:'log_producao', sql:`SELECT p.CDFIL,p.NRRQU,TRIM(p.SERIER) SERIER,p.DATA DATA_MOV,p.HORA HORA_MOV,TRIM(p.CDETAPA) COD_ETAPA,
      COALESCE(TRIM(p.CDOPERA),'(N/A)') COD_OPERACAO,COALESCE(TRIM(p.CDETAPAANT),'(N/A)') COD_ETAPA_ANT,p.DATAANT DATA_ETAPA_ANT,
      CASE WHEN p.DATAANT IS NULL OR p.HORAANT IS NULL THEN NULL ELSE DATEDIFF(MINUTE FROM (p.DATAANT+p.HORAANT) TO (p.DATA+p.HORA)) END MIN_NA_ETAPA_ANT,
      CASE WHEN TRIM(p.CDETAPA)='03' AND TRIM(p.CDOPERA)='02' THEN 'S' ELSE 'N' END IND_SAIDA_LAB,
      p.PRIORIDADE PRIORIDADE,TRIM(p.CDUSU) USUARIO,COALESCE(TRIM(p.CDMOTIVO),'(N/A)') COD_MOTIVO,TRIM(p.FINALIZADA) FINALIZADA_ERP,TRIM(p.ORIGEM) ORIGEM,TRIM(p.TPPCP) TIPO_PCP
    FROM FC12500 p WHERE p.DATA>=${DESDE}` },

 // ---------- ESTOQUE / AJUSTE / COMPRA (entraram em set/2026) ----------
 // ATENCAO ao mexer aqui: validar() trata tabela com 0 linhas como FALHA DURA e
 // aborta a carga inteira. Nunca incluir tabela que possa vir vazia — FC19100
 // (itens de perda) e FC11010 tem ZERO linhas e travariam o ETL para sempre.
 //
 // FC03100 e FC03140 sao FOTO DO AGORA: nao levam filtro de data porque nao
 // guardam historico. O ERP tinha um historico (FC03110) e ele parou de ser
 // alimentado quando a farmacia deixou de fechar o mes (ver FC03190: maio foi o
 // ultimo fechado). Por isso existe mart.estoque_snapshot.
 { name:'estoque_saldo', sql:`SELECT CDFIL,CDPRO,
     CAST(ESTAT AS DOUBLE PRECISION) ESTAT, CAST(ESTMI AS DOUBLE PRECISION) ESTMI,
     CAST(ESTMA AS DOUBLE PRECISION) ESTMA, CAST(PRCOMCTB AS DOUBLE PRECISION) PRCOMCTB
   FROM FC03100 WHERE CDFIL=1` },
 // SALDO REAL = ESTAT - SAIDATR. SAIDATR e o "transitorio": ja comprometido com
 // formula em producao. A propria tela do ERP mostra assim (Estoque Total,
 // Transitorio, Disponivel). Conferido: nenhum lote tem ESTAT<=0 com SAIDATR>0,
 // entao somar por lote reproduz o saldo do produto — bate em 1.415 de 1.415.
 { name:'estoque_lote', sql:`SELECT CDFIL,CDPRO,CTLOT,TRIM(NRLOT) NRLOT,TRIM(STLOT) STLOT,
     DTFAB,DTVAL,DTENT,NRNOT,TRIM(FORNECID) FORNECID,
     CAST(ESTAT AS DOUBLE PRECISION) ESTAT, CAST(SAIDATR AS DOUBLE PRECISION) SAIDATR
   FROM FC03140 WHERE CDFIL=1` },
 // FC1D000 = tela "Entradas > Conferencia Estoque". O campo MOTIVO existe e vem
 // SEMPRE em branco (2.170 lancamentos em 2026, zero preenchidos), por isso a
 // causa e inferida em mart.estoque_ajuste. USERIDAUT diz quem lancou.
 { name:'estoque_ajuste', sql:`SELECT ITEMID,DATARF,CDFIL,CDPRO,TRIM(NRLOT) NRLOT,
     CAST(QUANTDIFERENCA AS DOUBLE PRECISION) QUANTDIFERENCA,
     CAST(QUANTORIGINAL AS DOUBLE PRECISION) QUANTORIGINAL,
     CAST(QUANTAJUSTADA AS DOUBLE PRECISION) QUANTAJUSTADA,
     TRIM(MOTIVO) MOTIVO, OPERACAO, TRIM(USERIDAUT) USERIDAUT
   FROM FC1D000 WHERE CDFIL=1 AND DATARF>=${DESDE}` },
 { name:'compra_capa', sql:`SELECT CDFIL,TRIM(FORNECID) FORNECID,DTENT,NRNOT,NRPED,CFOP,
     CAST(VRTOT AS DOUBLE PRECISION) VRTOT, CAST(VRDSC AS DOUBLE PRECISION) VRDSC,
     CAST(VRTOTPROD AS DOUBLE PRECISION) VRTOTPROD
   FROM FC11000 WHERE CDFIL=1 AND DTENT>=${DESDE}` },
 // QUANT/PRUNI estao na unidade de COMPRA (ex.: 1 KG a R$ 300); QTREAL/PRUNIR na
 // unidade de ESTOQUE (1000 G a R$ 0,30). Valor = QUANT*PRUNI ou QTREAL*PRUNIR —
 // NUNCA cruzado: QTREAL*PRUNI da R$ 3,1 mi no ano contra R$ 1,25 mi das notas.
 // VRTOTPROD da capa vem sempre zerado; a conferencia e contra VRTOT.
 { name:'compra_item', sql:`SELECT CDFIL,TRIM(FORNECID) FORNECID,DTENT,NRNOT,ITEMID,CDPRO,
     TRIM(NRLOT) NRLOT, TRIM(UNIDA) UNIDA, DTFAB, DTVAL,
     CAST(QUANT AS DOUBLE PRECISION) QUANT, CAST(QTREAL AS DOUBLE PRECISION) QTREAL,
     CAST(PRUNI AS DOUBLE PRECISION) PRUNI, CAST(PRUNIR AS DOUBLE PRECISION) PRUNIR
   FROM FC11100 WHERE CDFIL=1 AND DTENT>=${DESDE}` },
 // FC02000 = cadastro de FORNECEDORES. Sem ela o portal só mostra o código (FORNECID das notas).
 // FANTA é o nome que o time usa (SOVITA, FAGRON, GALENA...). DIASPRAZO = prazo cadastrado;
 // VRMINFAT = pedido mínimo. Exige GRANT SELECT ON FC02000 TO SINEQUA_RO (grant-fornecedor.js).
 { name:'dim_fornecedor', sql:`SELECT TRIM(FORNECID) FORNECID, TRIM(RAZAO) RAZAO, TRIM(FANTA) FANTA,
     TRIM(NRCNPJ) NRCNPJ, TRIM(MUNIC) MUNIC, TRIM(UNFED) UNFED,
     CAST(DIASPRAZO AS DOUBLE PRECISION) DIASPRAZO, CAST(VRMINFAT AS DOUBLE PRECISION) VRMINFAT
   FROM FC02000` },
 // FC11200 = DUPLICATAS das notas de compra (parcelas a pagar: vencimento + valor). É o contas a
 // pagar das COMPRAS — prazo real de pagamento por fornecedor e o que vence nos próximos meses.
 // Tem lixo de digitação (DTDUP 1950): filtrar no uso, não aqui. Exige o mesmo GRANT (FC11200).
 { name:'compra_duplicata', sql:`SELECT CDFIL, TRIM(FORNECID) FORNECID, DTENT, NRNOT, DTDUP,
     CAST(VRDUP AS DOUBLE PRECISION) VRDUP, TRIM(NRDUP) NRDUP
   FROM FC11200 WHERE CDFIL=1 AND DTENT>=${DESDE}` },
];

// ---------- config de resiliência ----------
const QUERY_TIMEOUT_MS=180000, ATTACH_TIMEOUT_MS=30000, MAX_RETRY=2, RETRY_WAIT_MS=3000, BATCH=500;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function withTimeout(p,ms,label){ let t; const to=new Promise((_,rej)=>{ t=setTimeout(()=>rej(new Error(`timeout ${label} após ${ms}ms`)),ms); });
  return Promise.race([p,to]).finally(()=>clearTimeout(t)); }
function attachFB(){ return withTimeout(new Promise((res,rej)=>Firebird.attach(fbOpt,(e,d)=>e?rej(e):res(d))),ATTACH_TIMEOUT_MS,'attach'); }
function fbQuery(db,sql){ return withTimeout(new Promise((res,rej)=>db.query(sql,[],(e,r)=>e?rej(e):res(r))),QUERY_TIMEOUT_MS,'query'); }
async function safeDetach(db){ if(!db) return; try{ await withTimeout(new Promise(r=>db.detach(()=>r())),5000,'detach'); }catch{} }

const pad=n=>String(n).padStart(2,'0');
function fmtDate(x){ const Y=x.getFullYear(),M=pad(x.getMonth()+1),D=pad(x.getDate()),h=x.getHours(),m=x.getMinutes(),s=x.getSeconds();
  return (h||m||s)?`${Y}-${M}-${D} ${pad(h)}:${pad(m)}:${pad(s)}`:`${Y}-${M}-${D}`; }
const conv=v=> v==null?null : (v instanceof Date?fmtDate(v) : (typeof v==='string'?v.trim():v));

// grava as linhas em stg.<name>__new (não toca na tabela viva). Retorna nº de linhas (0 = não cria __new).
async function writeNew(pg,name,rows){
  const tgt=`stg.${name}__new`;
  await pg.query(`DROP TABLE IF EXISTS ${tgt}`);
  if(!rows.length) return 0;
  const cols=Object.keys(rows[0]);
  const kind={};
  for(const c of cols){ let s=null;
    for(const r of rows){ const v=r[c]; if(v==null) continue;
      let t=(v instanceof Date)?((v.getHours()||v.getMinutes()||v.getSeconds())?'ts':'date'):(typeof v==='number')?'num':'text';
      if(s==null) s=t; else if(s!==t){ s=((s==='date'&&t==='ts')||(s==='ts'&&t==='date'))?'ts':'text'; } }
    kind[c]= s==='num'?'double precision': s==='date'?'date': s==='ts'?'timestamp':'text';
  }
  const defs=cols.map(c=>`"${c.toLowerCase()}" ${kind[c]}`).join(', ');
  await pg.query(`CREATE TABLE ${tgt} (${defs})`);
  const lc=cols.map(c=>`"${c.toLowerCase()}"`), n=cols.length;
  for(let i=0;i<rows.length;i+=BATCH){ const sl=rows.slice(i,i+BATCH);
    const ph=sl.map((_,k)=>'('+cols.map((_,c)=>`$${k*n+c+1}`).join(',')+')').join(',');
    const vals=sl.flatMap(r=> cols.map(c=> conv(r[c])));
    await pg.query(`INSERT INTO ${tgt}(${lc.join(',')}) VALUES ${ph}`, vals);
  }
  return rows.length;
}

// assinatura de colunas (nome:tipo, na ordem) p/ decidir TRUNCATE+INSERT vs recriar
async function colsig(pg,table){
  const r=await pg.query(`SELECT column_name,data_type FROM information_schema.columns
    WHERE table_schema='stg' AND table_name=$1 ORDER BY ordinal_position`,[table]);
  return r.rows.length ? r.rows.map(x=>x.column_name+':'+x.data_type).join('|') : null;
}
async function dropNews(pg,names){ for(const n of names){ try{ await pg.query(`DROP TABLE IF EXISTS stg.${n}__new`); }catch{} } }

// ---------- canário de validação (roda no __new, ANTES do swap) ----------
const MIN_RATIO=0.90;      // __new deve ter >= 90% das linhas vivas (fatos são cumulativos, não encolhem)
const MAX_DIAS_PARADO=2;   // venda mais recente não pode ser mais velha que isso
async function validar(pg, staged, vazias){
  const hard=[], warn=[];
  const num=r=>Number(r.rows[0][Object.keys(r.rows[0])[0]]);
  // 1) HARD: toda tabela esperada tem que ter vindo com dados
  for(const name of jobs.map(j=>j.name))
    if(vazias.includes(name)) hard.push(`${name}: 0 linhas (esperado ter dados) — Firebird pode ter falhado`);
  // 2) HARD: regressão de linhas (new vs vivo)
  for(const name of staged){
    const exists=(await pg.query(`SELECT to_regclass('stg.${name}') r`)).rows[0].r;
    if(!exists) continue;  // primeira carga desta tabela
    const live=num(await pg.query(`SELECT count(*) FROM stg.${name}`));
    const nw  =num(await pg.query(`SELECT count(*) FROM stg.${name}__new`));
    if(live>0 && nw < live*MIN_RATIO) hard.push(`${name}: ${nw} linhas < ${Math.round(MIN_RATIO*100)}% das ${live} atuais`);
  }
  // 3) SOFT: frescor + receita do mês corrente (só avisa)
  try{
    const fresh=(await pg.query(`SELECT max(dtentr)::date d FROM stg.venda_formula__new`)).rows[0].d;
    if(fresh){
      const atras=num(await pg.query(`SELECT (CURRENT_DATE - $1::date)`,[fresh]));
      if(atras>MAX_DIAS_PARADO) warn.push(`venda mais recente em ${fresh} (${atras} dias atrás) — dados podem estar parados`);
    }
    const mesFiltro=`date_trunc('month',dtentr)=date_trunc('month',(SELECT max(dtentr) FROM stg.venda_formula__new))`;
    const vNew =num(await pg.query(`SELECT COALESCE(SUM(prcobr),0) FROM stg.venda_formula__new WHERE ${mesFiltro}`));
    const vLive=num(await pg.query(`SELECT COALESCE(SUM(prcobr),0) FROM stg.venda_formula WHERE ${mesFiltro}`));
    if(vLive>0 && vNew < vLive*0.95)
      warn.push(`venda bruta do mês caiu p/ ${Math.round(vNew).toLocaleString('pt-BR')} (era ${Math.round(vLive).toLocaleString('pt-BR')})`);
  }catch(e){ warn.push('checagem de negócio pulada: '+e.message.slice(0,50)); }
  return {hard, warn};
}

// puxa 1 job com timeout + retry (reconecta a Firebird em caso de hang/erro)
async function stageJob(st,pg,job){
  for(let attempt=0; attempt<=MAX_RETRY; attempt++){
    try{
      if(!st.db) st.db=await attachFB();
      const rows=await fbQuery(st.db,job.sql);
      return await writeNew(pg,job.name,rows);
    }catch(e){
      const msg=e.message.slice(0,70);
      await safeDetach(st.db); st.db=null;               // conexão pode estar zumbi -> descarta
      if(attempt>=MAX_RETRY) throw new Error(`${job.name}: ${msg}`);
      console.log(`  ⚠️  ${job.name.padEnd(20)} tentativa ${attempt+1} falhou (${msg}) — reconectando`);
      await sleep(RETRY_WAIT_MS);
    }
  }
}

async function main(){
  const pg=new Client({host:'localhost',port:5432,database:'sinequa',user:process.env.USER||require('os').userInfo().username});
  await pg.connect();
  await pg.query('CREATE SCHEMA IF NOT EXISTS stg');
  await pg.query('CREATE SCHEMA IF NOT EXISTS mart');
  console.log(`Extração ${new Date().toLocaleString('pt-BR')}\n`);
  const st={ db:null };
  const staged=[], vazias=[];
  // ----- fase 1: stage em __new (aborta na 1ª falha, sem tocar nos dados vivos) -----
  try{
    for(const j of jobs){
      const n=await stageJob(st,pg,j);
      if(n>0){ staged.push(j.name); console.log(`  ✔ stg.${j.name.padEnd(22)} ${n} linhas (staged)`); }
      else   { vazias.push(j.name); console.log(`  ⚠️  stg.${j.name.padEnd(22)} 0 linhas — mantendo dados atuais`); }
    }
  }catch(e){
    await safeDetach(st.db); await dropNews(pg,jobs.map(j=>j.name)); await pg.end();
    console.error(`\n❌ Extração abortada: ${e.message}\n   Dados vivos preservados (nenhuma troca feita).`);
    process.exit(1);
  }
  await safeDetach(st.db);

  // ----- fase 1.5: canário de validação (gate antes do swap) -----
  const {hard, warn} = await validar(pg, staged, vazias);
  for(const w of warn) console.log(`  ⚠️  AVISO: ${w}`);
  if(hard.length){
    await dropNews(pg,jobs.map(j=>j.name)); await pg.end();
    console.error(`\n❌ Validação reprovou — swap cancelado, dados vivos preservados:`);
    for(const h of hard) console.error(`   • ${h}`);
    process.exit(1);
  }

  // ----- fase 2: swap transacional -----
  let schemaChanged=false;
  try{
    await pg.query('BEGIN');
    await pg.query("SET LOCAL lock_timeout='45s'");   // se um leitor segurar demais, falha e faz rollback
    for(const name of staged){
      const live=await colsig(pg,name), nw=await colsig(pg,name+'__new');
      if(live && live===nw){                          // mesma estrutura -> troca sem derrubar as views
        await pg.query(`TRUNCATE stg.${name}`);
        await pg.query(`INSERT INTO stg.${name} SELECT * FROM stg.${name}__new`);
        await pg.query(`DROP TABLE stg.${name}__new`);
      }else{                                          // tabela nova ou schema mudou -> recria (mart precisa rebuild)
        await pg.query(`DROP TABLE IF EXISTS stg.${name} CASCADE`);
        await pg.query(`ALTER TABLE stg.${name}__new RENAME TO ${name}`);
        schemaChanged=true;
      }
    }
    await pg.query('CREATE TABLE IF NOT EXISTS mart.atualizacao (atualizado_em timestamptz)');
    await pg.query('TRUNCATE mart.atualizacao');
    await pg.query('INSERT INTO mart.atualizacao VALUES (now())');
    await pg.query('COMMIT');
  }catch(e){
    try{ await pg.query('ROLLBACK'); }catch{}
    await dropNews(pg,jobs.map(j=>j.name)); await pg.end();
    console.error(`\n❌ Falha no swap: ${e.message}\n   ROLLBACK — dados vivos preservados.`);
    process.exit(1);
  }
  // ----- fase 3: foto diária do estoque -----
  // FORA da transação do swap, e de propósito: se isto falhar (view ainda não
  // criada numa primeira carga, p.ex.), a carga já está commitada e não pode ser
  // desfeita por causa do histórico. Só avisa.
  // ON CONFLICT DO UPDATE faz a última execução do dia prevalecer, então roda a
  // cada ciclo e converge para o fechamento — sem agendamento próprio.
  try{
    const snap=await pg.query(`INSERT INTO mart.estoque_snapshot
        (data, cdpro, saldo, valor, situacao, dias_cobertura, valor_vencido, etl_em)
      SELECT current_date, s.cdpro, s.saldo, s.valor, s.situacao, s.dias_cobertura,
             s.valor_vencido, (SELECT max(atualizado_em) FROM mart.atualizacao)
        FROM mart.estoque_sku s
      ON CONFLICT (data, cdpro) DO UPDATE SET
        saldo=EXCLUDED.saldo, valor=EXCLUDED.valor, situacao=EXCLUDED.situacao,
        dias_cobertura=EXCLUDED.dias_cobertura, valor_vencido=EXCLUDED.valor_vencido,
        etl_em=EXCLUDED.etl_em`);
    console.log(`  📸 snapshot de estoque: ${snap.rowCount} SKUs`);
  }catch(e){ console.log(`  ⚠️  snapshot de estoque pulado: ${e.message.slice(0,80)}`); }

  await pg.end();
  console.log(`\n✅ ${staged.length} tabelas trocadas`
    + (vazias.length?`, ${vazias.length} mantidas (0 linhas)`:'')
    + (warn.length?`  (${warn.length} aviso(s))`:'')
    + (schemaChanged?'  ⚠️ schema mudou → recriar mart':''));
  process.exit(schemaChanged ? 10 : (warn.length ? 12 : 0));
}

if(require.main===module) main().catch(e=>{ console.error('❌ fatal',e.message); process.exit(1); });
module.exports={ validar, jobs };
