"""
gerar_pagina.py — FULLBETS v3
Gera docs/index.html e docs/sinais.json
"""
import json, os
from datetime import datetime
from pathlib import Path
import pandas as pd

ARQUIVO_SINAIS = "sinais_hoje.xlsx"
PASTA_DOCS     = Path("docs")
ARQUIVO_HTML   = PASTA_DOCS / "index.html"
ARQUIVO_JSON   = PASTA_DOCS / "sinais.json"

def hora_str(h):
    if h is None: return ""
    try:
        ts = float(h)
        if ts > 1_000_000_000:
            from datetime import timezone, timedelta
            tz_br = timezone(timedelta(hours=-3))
            return datetime.fromtimestamp(ts, tz=tz_br).strftime("%H:%M")
        return str(h)[:5]
    except:
        return str(h)[:5]

def carregar_sinais():
    if not os.path.exists(ARQUIVO_SINAIS): return []
    df = pd.read_excel(ARQUIVO_SINAIS)
    df = df.where(pd.notnull(df), None)
    sinais = df.to_dict("records")
    for s in sinais:
        s["Hora"] = hora_str(s.get("Hora"))
    sinais.sort(key=lambda r: (str(r.get("Data") or ""), str(r.get("Hora") or "")))
    return sinais

def main():
    PASTA_DOCS.mkdir(exist_ok=True)
    sinais  = carregar_sinais()
    agora   = datetime.now().strftime("%d/%m/%Y %H:%M")
    hoje    = datetime.now().strftime("%d/%m/%Y")
    n_apto  = sum(1 for s in sinais if str(s.get("APTO","")).upper() == "SIM")
    total   = len(sinais)
    payload = {"atualizado": agora, "total": total, "n_apto": n_apto, "sinais": sinais}

    # Salvar JSON com ensure_ascii para evitar qualquer problema de encoding
    with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, default=str)

    # Gerar JSON string segura para embutir no HTML
    json_safe = json.dumps(payload, ensure_ascii=True, default=str)
    # Escapar </script> para não quebrar o HTML
    json_safe = json_safe.replace("</", "<\\/")

    ligas = sorted(set(s.get("Liga","") for s in sinais if s.get("Liga")))
    opts  = "\n".join(f'<option value="{l}">{l}</option>' for l in ligas)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FULLBETS ML</title>
<style>
:root{{--bg:#0d1117;--bg2:#161b22;--bg3:#21262d;--green:#3fb950;--red:#f85149;--blue:#58a6ff;--yellow:#e3b341;--text:#c9d1d9;--muted:#8b949e;--border:#30363d;}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,sans-serif;min-height:100vh;}}
header{{background:var(--bg2);border-bottom:1px solid var(--border);padding:18px;text-align:center;}}
header h1{{font-size:1.5rem;color:var(--blue);letter-spacing:3px;font-weight:700;}}
header .sub{{color:var(--muted);font-size:.78rem;margin-top:4px;}}
.kpis{{display:flex;gap:1px;background:var(--border);border-bottom:1px solid var(--border);}}
.kpi{{flex:1;background:var(--bg2);padding:12px;text-align:center;}}
.kpi .val{{font-size:1.7rem;font-weight:700;}}
.kpi .lbl{{color:var(--muted);font-size:.72rem;margin-top:2px;}}
.green{{color:var(--green);}} .blue{{color:var(--blue);}} .gray{{color:var(--muted);}}
.controles{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:12px 20px;background:var(--bg2);border-bottom:1px solid var(--border);max-width:980px;margin:0 auto;}}
.tabs{{display:flex;gap:6px;flex:1;}}
.tab{{padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:transparent;color:var(--muted);font-size:.78rem;cursor:pointer;transition:all .15s;}}
.tab.active{{background:var(--blue);color:#fff;border-color:var(--blue);font-weight:600;}}
.nb{{background:rgba(255,255,255,.12);border-radius:8px;padding:1px 5px;font-size:.68rem;margin-left:3px;}}
select{{background:var(--bg3);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:5px 10px;font-size:.78rem;cursor:pointer;}}
.section{{padding:14px 20px;max-width:980px;margin:0 auto;}}
.grid{{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));}}
.card{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px;cursor:pointer;transition:border-color .15s,transform .1s;}}
.card:hover{{border-color:var(--blue);transform:translateY(-2px);}}
.card.apto{{border-left:3px solid var(--green);}}
.card.napt{{border-left:3px solid var(--border);opacity:.78;}}
.ctop{{display:flex;align-items:center;gap:6px;margin-bottom:7px;flex-wrap:wrap;}}
.liga{{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;flex:1;}}
.badge{{font-size:.65rem;font-weight:700;padding:2px 7px;border-radius:8px;white-space:nowrap;}}
.badge.apto{{background:rgba(63,185,80,.15);color:var(--green);}}
.badge.napt{{background:rgba(248,81,73,.1);color:var(--red);}}
.dt{{font-size:.65rem;color:var(--muted);background:var(--bg3);padding:2px 7px;border-radius:4px;white-space:nowrap;}}
.teams{{font-size:.95rem;font-weight:600;margin-bottom:8px;}}
.vs{{color:var(--muted);margin:0 5px;font-weight:400;}}
.probs{{display:flex;gap:10px;margin-bottom:5px;flex-wrap:wrap;}}
.prob{{font-size:.78rem;}}
.p10{{color:var(--blue);font-weight:700;}}
.p15{{color:var(--green);font-weight:700;}}
.p5{{color:var(--muted);font-weight:700;}}
.sh{{font-size:.7rem;color:var(--muted);}}
.empty{{text-align:center;padding:50px;color:var(--muted);}}
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:999;align-items:center;justify-content:center;padding:16px;}}
.overlay.open{{display:flex;}}
.modal{{background:var(--bg2);border:1px solid var(--border);border-radius:12px;width:100%;max-width:520px;max-height:92vh;overflow-y:auto;}}
.mhdr{{padding:16px 18px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;gap:10px;}}
.mtitle{{flex:1;}}
.mliga{{font-size:.68rem;color:var(--muted);text-transform:uppercase;margin-bottom:4px;}}
.mteams{{font-size:1.05rem;font-weight:700;}}
.mclose{{background:none;border:none;color:var(--muted);font-size:1.1rem;cursor:pointer;padding:2px 6px;}}
.mbody{{padding:16px 18px;}}
.msec{{margin-bottom:16px;}}
.mstitle{{font-size:.68rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid var(--border);padding-bottom:5px;margin-bottom:10px;}}
.ebox{{background:rgba(63,185,80,.07);border:1px solid rgba(63,185,80,.2);border-radius:8px;padding:12px;}}
.erow{{display:flex;justify-content:space-between;align-items:center;padding:4px 0;font-size:.82rem;border-bottom:1px solid rgba(255,255,255,.04);}}
.erow:last-child{{border-bottom:none;}}
.el{{color:var(--muted);}} .ev{{font-weight:600;}}
.evg{{color:var(--green);}} .evb{{color:var(--blue);}} .evy{{color:var(--yellow);}}
.pbars{{display:flex;flex-direction:column;gap:8px;}}
.prow{{display:flex;align-items:center;gap:8px;font-size:.78rem;}}
.plbl{{width:28px;color:var(--muted);}}
.ptrack{{flex:1;background:var(--bg3);border-radius:4px;height:8px;overflow:hidden;}}
.pfill{{height:100%;border-radius:4px;}}
.pval{{width:44px;text-align:right;font-weight:700;}}
.igrid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.iitem{{background:var(--bg3);border-radius:6px;padding:9px 12px;}}
.ilbl{{font-size:.65rem;color:var(--muted);margin-bottom:3px;}}
.ival{{font-size:.92rem;font-weight:600;}}
.crits{{display:flex;flex-direction:column;gap:7px;}}
.crit{{display:flex;align-items:center;gap:8px;font-size:.8rem;}}
.ico{{width:18px;font-size:.85rem;}}
.clbl{{flex:1;color:var(--muted);}}
.cval{{font-weight:700;}}
.mbox{{background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.2);border-radius:6px;padding:10px;font-size:.78rem;color:var(--red);}}
footer{{text-align:center;color:var(--muted);font-size:.7rem;padding:20px;border-top:1px solid var(--border);margin-top:8px;}}
</style>
</head>
<body>
<header>
  <h1>FULLBETS ML</h1>
  <div class="sub">Over 0.5 HT &mdash; {hoje}</div>
  <div class="sub">Atualizado: {agora} &nbsp;&middot;&nbsp; m10&ge;0.55 &middot; m15&ge;0.52 &middot; LG_C&ge;100 &middot; Emp&ge;3.5</div>
</header>
<div class="kpis">
  <div class="kpi"><div class="val blue">{total}</div><div class="lbl">analisados</div></div>
  <div class="kpi"><div class="val green">{n_apto}</div><div class="lbl">APTOS</div></div>
  <div class="kpi"><div class="val gray">{total - n_apto}</div><div class="lbl">outros</div></div>
</div>
<div class="controles">
  <div class="tabs">
    <button class="tab active" data-tab="apto" onclick="setTab(this)">APTOS <span class="nb">{n_apto}</span></button>
    <button class="tab" data-tab="todos" onclick="setTab(this)">Todos <span class="nb">{total}</span></button>
  </div>
  <div style="display:flex;align-items:center;gap:6px;">
    <label style="font-size:.78rem;color:var(--muted);">Liga:</label>
    <select id="fl" onchange="render()">
      <option value="">Todas</option>
      {opts}
    </select>
  </div>
</div>
<div class="section"><div id="grid" class="grid"></div></div>
<div class="overlay" id="ov" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="mhdr">
      <div class="mtitle"><div class="mliga" id="ml"></div><div class="mteams" id="mt"></div></div>
      <button class="mclose" onclick="closeModal()">&#x2715;</button>
    </div>
    <div class="mbody">
      <div class="msec"><div class="mstitle">Momento de Entrada</div><div class="ebox" id="me"></div></div>
      <div class="msec"><div class="mstitle">Probabilidades</div><div class="pbars" id="mp"></div></div>
      <div class="msec"><div class="mstitle">Dados Sherlock</div><div class="igrid" id="ms"></div></div>
      <div class="msec"><div class="mstitle">Criterios de Entrada</div><div class="crits" id="mc"></div></div>
      <div class="msec" id="mmotiv" style="display:none"><div class="mstitle">Motivo Reprovacao</div><div class="mbox" id="mmsg"></div></div>
    </div>
  </div>
</div>
<footer>FULLBETS ML &middot; tarcisio248 &middot; 2026<br>Entrada min10 &middot; Saida min35 &middot; ROI +14% &middot; 13/13 semanas positivas<br>Decisao final e sempre manual.</footer>
<script>
var D={json_safe};
var S=D.sinais.map(function(s,i){{s._i=i;return s;}});
var tab='apto';
function setTab(b){{document.querySelectorAll('.tab').forEach(function(t){{t.classList.remove('active');}});b.classList.add('active');tab=b.dataset.tab;render();}}
function fmt(v,d){{if(v==null||isNaN(v))return'--';return parseFloat(v).toFixed(d||3);}}
function fi(v){{if(v==null||isNaN(v))return'--';return Math.round(parseFloat(v));}}
function bc(v){{if(v>=0.75)return'var(--green)';if(v>=0.60)return'var(--blue)';if(v>=0.50)return'var(--yellow)';return'var(--red)';}}
function render(){{
  var liga=document.getElementById('fl').value;
  var list=S.filter(function(s){{return !liga||s.Liga===liga;}});
  if(tab==='apto')list=list.filter(function(s){{return String(s.APTO||'').toUpperCase()==='SIM';}});
  list.sort(function(a,b){{
    var ka=String(a.Data||'')+String(a.Hora||'');
    var kb=String(b.Data||'')+String(b.Hora||'');
    return ka<kb?-1:ka>kb?1:0;
  }});
  var g=document.getElementById('grid');
  if(!list.length){{g.innerHTML='<div class="empty">Nenhum jogo encontrado.</div>';return;}}
  g.innerHTML=list.map(function(s){{
    var ap=String(s.APTO||'').toUpperCase()==='SIM';
    var dt=s.Data?String(s.Data).slice(0,10).split('-').reverse().join('/'):'--';
    return '<div class="card '+(ap?'apto':'napt')+'" onclick="openModal('+s._i+')">'
      +'<div class="ctop"><span class="liga">'+String(s.Liga||'')+'</span>'
      +'<span class="badge '+(ap?'apto':'napt')+'">'+(ap?'APTO':'X')+'</span>'
      +'<span class="dt">'+dt+' '+String(s.Hora||'')+'</span></div>'
      +'<div class="teams">'+String(s.Casa||'')+'<span class="vs">x</span>'+String(s.Visitante||'')+'</div>'
      +'<div class="probs"><span class="prob">m10: <b class="p10">'+fmt(s.m10)+'</b></span>'
      +'<span class="prob">m15: <b class="p15">'+fmt(s.m15)+'</b></span>'
      +'<span class="prob">m5: <b class="p5">'+fmt(s.m5)+'</b></span></div>'
      +'<div class="sh">LG_C='+fi(s.LG_C)+' H='+fi(s.H_Score_C)+' Emp='+fmt(s.Odd_Emp,1)+'</div>'
      +'</div>';
  }}).join('');
}}
function openModal(i){{
  var s=S[i];
  var ap=String(s.APTO||'').toUpperCase()==='SIM';
  var dt=s.Data?String(s.Data).slice(0,10).split('-').reverse().join('/'):'--';
  document.getElementById('ml').textContent=String(s.Liga||'');
  document.getElementById('mt').textContent=String(s.Casa||'')+' x '+String(s.Visitante||'');
  document.getElementById('me').innerHTML=
    '<div class="erow"><span class="el">Data/Hora</span><span class="ev evy">'+dt+' '+String(s.Hora||'')+'</span></div>'
    +'<div class="erow"><span class="el">Mercado</span><span class="ev">Over 0.5 HT</span></div>'
    +'<div class="erow"><span class="el">Entrada</span><span class="ev evg">Min10 (0x0)</span></div>'
    +'<div class="erow"><span class="el">Saida/Red</span><span class="ev">Min35</span></div>'
    +'<div class="erow"><span class="el">Odd Empate</span><span class="ev evb">'+fmt(s.Odd_Emp,2)+'</span></div>'
    +'<div class="erow"><span class="el">Prob m10</span><span class="ev evg">'+(s.m10!=null?(s.m10*100).toFixed(1)+'%':'--')+'</span></div>'
    +'<div class="erow"><span class="el">Prob m15</span><span class="ev evb">'+(s.m15!=null?(s.m15*100).toFixed(1)+'%':'--')+'</span></div>';
  var probs=[['m5',s.m5],['m10',s.m10],['m15',s.m15]];
  document.getElementById('mp').innerHTML=probs.map(function(p){{
    if(p[1]==null)return'';
    var pct=Math.min(100,p[1]*100).toFixed(1);
    var c=bc(p[1]);
    return '<div class="prow"><span class="plbl">'+p[0]+'</span>'
      +'<div class="ptrack"><div class="pfill" style="width:'+pct+'%;background:'+c+'"></div></div>'
      +'<span class="pval" style="color:'+c+'">'+pct+'%</span></div>';
  }}).join('');
  document.getElementById('ms').innerHTML=
    '<div class="iitem"><div class="ilbl">LG Casa</div><div class="ival">'+fi(s.LG_C)+'</div></div>'
    +'<div class="iitem"><div class="ilbl">LG Visit</div><div class="ival">'+fi(s.LG_V)+'</div></div>'
    +'<div class="iitem"><div class="ilbl">H Score</div><div class="ival">'+fi(s.H_Score_C)+'</div></div>'
    +'<div class="iitem"><div class="ilbl">Odd Casa</div><div class="ival">'+fmt(s.Odd_Casa,2)+'</div></div>'
    +'<div class="iitem"><div class="ilbl">Odd Emp</div><div class="ival">'+fmt(s.Odd_Emp,2)+'</div></div>'
    +'<div class="iitem"><div class="ilbl">Odd Visit</div><div class="ival">'+fmt(s.Odd_Visit,2)+'</div></div>';
  var cr=[
    ['m10 >= 0.55',s.m10!=null&&s.m10>=0.55,fmt(s.m10)],
    ['m15 >= 0.52',s.m15!=null&&s.m15>=0.52,fmt(s.m15)],
    ['LG_C >= 100',s.LG_C!=null&&s.LG_C>=100,fi(s.LG_C)],
    ['Odd_Emp >= 3.5',s.Odd_Emp!=null&&s.Odd_Emp>=3.5,fmt(s.Odd_Emp,2)]
  ];
  document.getElementById('mc').innerHTML=cr.map(function(c){{
    return '<div class="crit"><span class="ico">'+(c[1]?'OK':'XX')+'</span><span class="clbl">'+c[0]+'</span><span class="cval" style="color:'+(c[1]?'var(--green)':'var(--red)')+'">'+c[2]+'</span></div>';
  }}).join('');
  var ms=document.getElementById('mmotiv');
  if(!ap&&s.MOTIVO&&s.MOTIVO!=='OK'){{
    document.getElementById('mmsg').textContent=String(s.MOTIVO||'');
    ms.style.display='block';
  }}else{{ms.style.display='none';}}
  document.getElementById('ov').classList.add('open');
}}
function closeModal(){{document.getElementById('ov').classList.remove('open');}}
document.addEventListener('keydown',function(e){{if(e.key==='Escape')closeModal();}});
render();
</script>
</body>
</html>"""

    with open(ARQUIVO_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Pagina gerada: {ARQUIVO_HTML}")
    print(f"JSON gerado:   {ARQUIVO_JSON}")
    print(f"APTOS: {n_apto} | Total: {total}")

if __name__ == "__main__":
    main()
