"""
gerar_pagina.py — FULLBETS v2
==============================
Lê sinais_hoje.xlsx e gera docs/index.html + docs/sinais.json.

Melhorias v2:
  - Data e hora nos cards
  - Ordenação por data/hora
  - Filtro por liga
  - Modal com detalhes ao clicar (momento ideal, SH, critérios)

Uso:
    python gerar_pagina.py
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

ARQUIVO_SINAIS = "sinais_hoje.xlsx"
PASTA_DOCS     = Path("docs")
ARQUIVO_HTML   = PASTA_DOCS / "index.html"
ARQUIVO_JSON   = PASTA_DOCS / "sinais.json"


def _hora_legivel(h):
    """Converte timestamp Unix ou string para HH:MM."""
    if h is None:
        return ""
    try:
        ts = float(h)
        if ts > 1_000_000_000:  # timestamp Unix
            return datetime.utcfromtimestamp(ts).strftime("%H:%M")
        return str(h)[:5]
    except:
        return str(h)[:5]


def carregar_sinais() -> list:
    if not os.path.exists(ARQUIVO_SINAIS):
        return []
    df = pd.read_excel(ARQUIVO_SINAIS)
    df = df.where(pd.notnull(df), None)
    sinais = df.to_dict("records")
    # Converter campo Hora de timestamp Unix para HH:MM
    for s in sinais:
        s["Hora"] = _hora_legivel(s.get("Hora"))
    sinais.sort(key=lambda r: (str(r.get("Data") or ""), str(r.get("Hora") or "")))
    return sinais


def montar_json(sinais: list) -> dict:
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    apto  = [s for s in sinais if str(s.get("APTO","")).upper() == "SIM"]
    return {"atualizado": agora, "total": len(sinais), "n_apto": len(apto), "sinais": sinais}


def gerar_html(payload: dict) -> str:
    agora    = payload["atualizado"]
    total    = payload["total"]
    n_apto   = payload["n_apto"]
    sinais   = payload["sinais"]
    json_str = json.dumps(payload, ensure_ascii=False, default=str).replace("</", "<\\/")
    ligas    = sorted(set(s.get("Liga","?") for s in sinais if s.get("Liga")))
    opts     = "\n".join(f'<option value="{l}">{l}</option>' for l in ligas)
    hoje     = datetime.now().strftime("%d/%m/%Y")

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FULLBETS ML</title>
<style>
:root{{--bg:#0d1117;--bg2:#161b22;--bg3:#21262d;--green:#3fb950;--red:#f85149;--blue:#58a6ff;--yellow:#e3b341;--text:#c9d1d9;--muted:#8b949e;--border:#30363d;}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;}}
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
.n-badge{{background:rgba(255,255,255,.12);border-radius:8px;padding:1px 5px;font-size:.68rem;margin-left:3px;}}
.filter-wrap{{display:flex;align-items:center;gap:6px;}}
.filter-wrap label{{font-size:.78rem;color:var(--muted);white-space:nowrap;}}
select{{background:var(--bg3);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:5px 10px;font-size:.78rem;cursor:pointer;}}
.section{{padding:14px 20px;max-width:980px;margin:0 auto;}}
.cards-grid{{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));}}
.card{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px;cursor:pointer;transition:border-color .15s,transform .1s,box-shadow .15s;}}
.card:hover{{border-color:var(--blue);transform:translateY(-2px);box-shadow:0 4px 16px rgba(88,166,255,.12);}}
.card.apto{{border-left:3px solid var(--green);}}
.card.napt{{border-left:3px solid var(--border);opacity:.78;}}
.card-top{{display:flex;align-items:center;gap:6px;margin-bottom:7px;flex-wrap:wrap;}}
.liga-tag{{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;flex:1;}}
.badge{{font-size:.65rem;font-weight:700;padding:2px 7px;border-radius:8px;white-space:nowrap;}}
.badge.apto{{background:rgba(63,185,80,.15);color:var(--green);}}
.badge.napt{{background:rgba(248,81,73,.1);color:var(--red);}}
.dt-tag{{font-size:.65rem;color:var(--muted);background:var(--bg3);padding:2px 7px;border-radius:4px;white-space:nowrap;}}
.teams{{font-size:.95rem;font-weight:600;margin-bottom:8px;line-height:1.4;}}
.vs{{color:var(--muted);margin:0 5px;font-weight:400;}}
.probs{{display:flex;gap:10px;margin-bottom:5px;flex-wrap:wrap;}}
.prob{{font-size:.78rem;}}
.p10{{color:var(--blue);font-weight:700;}}
.p15{{color:var(--green);font-weight:700;}}
.p5{{color:var(--muted);font-weight:700;}}
.sh-row{{font-size:.7rem;color:var(--muted);}}
.empty{{text-align:center;padding:50px;color:var(--muted);font-size:.88rem;}}

/* MODAL */
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:999;align-items:center;justify-content:center;padding:16px;}}
.overlay.open{{display:flex;}}
.modal{{background:var(--bg2);border:1px solid var(--border);border-radius:12px;width:100%;max-width:520px;max-height:92vh;overflow-y:auto;}}
.mhdr{{padding:16px 18px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;gap:10px;}}
.mtitle{{flex:1;}}
.mliga{{font-size:.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px;}}
.mteams{{font-size:1.05rem;font-weight:700;}}
.mclose{{background:none;border:none;color:var(--muted);font-size:1.1rem;cursor:pointer;padding:2px 6px;line-height:1;}}
.mclose:hover{{color:var(--text);}}
.mbody{{padding:16px 18px;}}
.msec{{margin-bottom:16px;}}
.msec-title{{font-size:.68rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid var(--border);padding-bottom:5px;margin-bottom:10px;}}
.entry-box{{background:rgba(63,185,80,.07);border:1px solid rgba(63,185,80,.2);border-radius:8px;padding:12px;}}
.erow{{display:flex;justify-content:space-between;align-items:center;padding:4px 0;font-size:.82rem;border-bottom:1px solid rgba(255,255,255,.04);}}
.erow:last-child{{border-bottom:none;}}
.erow .el{{color:var(--muted);}}
.erow .ev{{font-weight:600;}}
.ev.g{{color:var(--green);}} .ev.b{{color:var(--blue);}} .ev.y{{color:var(--yellow);}}
.pbars{{display:flex;flex-direction:column;gap:8px;}}
.pbar-row{{display:flex;align-items:center;gap:8px;font-size:.78rem;}}
.pbar-lbl{{width:28px;color:var(--muted);}}
.pbar-track{{flex:1;background:var(--bg3);border-radius:4px;height:8px;overflow:hidden;}}
.pbar-fill{{height:100%;border-radius:4px;transition:width .4s;}}
.pbar-val{{width:44px;text-align:right;font-weight:700;}}
.igrid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.iitem{{background:var(--bg3);border-radius:6px;padding:9px 12px;}}
.iitem .ilbl{{font-size:.65rem;color:var(--muted);margin-bottom:3px;}}
.iitem .ival{{font-size:.92rem;font-weight:600;}}
.crits{{display:flex;flex-direction:column;gap:7px;}}
.crit{{display:flex;align-items:center;gap:8px;font-size:.8rem;}}
.crit .ico{{width:18px;font-size:.85rem;}}
.crit .clbl{{flex:1;color:var(--muted);}}
.crit .cval{{font-weight:700;}}
.mbox{{background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.2);border-radius:6px;padding:10px;font-size:.78rem;color:var(--red);}}
footer{{text-align:center;color:var(--muted);font-size:.7rem;padding:20px;border-top:1px solid var(--border);margin-top:8px;}}
</style>
</head>
<body>
<header>
  <h1>FULLBETS ML</h1>
  <div class="sub">Over 0.5 HT &mdash; {hoje}</div>
  <div class="sub">Atualizado: {agora} &nbsp;·&nbsp; m10&ge;0.55 &middot; m15&ge;0.52 &middot; LG_C&ge;100 &middot; Emp&ge;3.5</div>
</header>

<div class="kpis">
  <div class="kpi"><div class="val blue">{total}</div><div class="lbl">analisados</div></div>
  <div class="kpi"><div class="val green">{n_apto}</div><div class="lbl">APTOS</div></div>
  <div class="kpi"><div class="val gray">{total - n_apto}</div><div class="lbl">outros</div></div>
</div>

<div class="controles">
  <div class="tabs">
    <button class="tab active" data-tab="apto" onclick="setTab(this)">
      ✔ APTOS <span class="n-badge">{n_apto}</span>
    </button>
    <button class="tab" data-tab="todos" onclick="setTab(this)">
      Todos <span class="n-badge">{total}</span>
    </button>
  </div>
  <div class="filter-wrap">
    <label>Liga:</label>
    <select id="filtro-liga" onchange="render()">
      <option value="">Todas</option>
      {opts}
    </select>
  </div>
</div>

<div class="section">
  <div id="grid" class="cards-grid"></div>
</div>

<!-- MODAL -->
<div class="overlay" id="overlay" onclick="overlayClick(event)">
  <div class="modal" id="modal">
    <div class="mhdr">
      <div class="mtitle">
        <div class="mliga" id="m-liga"></div>
        <div class="mteams" id="m-teams"></div>
      </div>
      <button class="mclose" onclick="closeModal()">&#x2715;</button>
    </div>
    <div class="mbody">
      <div class="msec">
        <div class="msec-title">&#x26A1; Momento de Entrada</div>
        <div class="entry-box" id="m-entry"></div>
      </div>
      <div class="msec">
        <div class="msec-title">&#x1F4CA; Probabilidades</div>
        <div class="pbars" id="m-probs"></div>
      </div>
      <div class="msec">
        <div class="msec-title">&#x1F50D; Dados Sherlock</div>
        <div class="igrid" id="m-sh"></div>
      </div>
      <div class="msec">
        <div class="msec-title">&#x2705; Crit&eacute;rios de Entrada</div>
        <div class="crits" id="m-crits"></div>
      </div>
      <div class="msec" id="m-motivo-sec" style="display:none">
        <div class="msec-title">&#x26A0;&#xFE0F; Motivo da Reprovação</div>
        <div class="mbox" id="m-motivo"></div>
      </div>
    </div>
  </div>
</div>

<footer>
  FULLBETS ML &middot; tarcisio248 &middot; 2026<br>
  Entrada min10 &middot; Sa&iacute;da min35 &middot; ROI hist&oacute;rico +14% &middot; 13/13 semanas positivas<br>
  Decis&atilde;o final de entrada &eacute; sempre manual.
</footer>

<script>
const DADOS = {json_str};
const S = DADOS.sinais.map((s,i) => ({{...s, _idx:i}}));
let tabAtual = 'apto';

function setTab(btn) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  tabAtual = btn.dataset.tab;
  render();
}}

function render() {{
  const liga = document.getElementById('filtro-liga').value;
  let lista = S.filter(s => !liga || s.Liga === liga);
  if (tabAtual === 'apto') lista = lista.filter(s => String(s.APTO||'').toUpperCase() === 'SIM');
  lista.sort((a,b) => {{
    const ka = (a.Data||'') + String(a.Hora||'').slice(0,5);
    const kb = (b.Data||'') + String(b.Hora||'').slice(0,5);
    return ka.localeCompare(kb);
  }});
  const grid = document.getElementById('grid');
  if (!lista.length) {{ grid.innerHTML = '<div class="empty">Nenhum jogo encontrado.</div>'; return; }}
  grid.innerHTML = lista.map(s => {{
    const apto = String(s.APTO||'').toUpperCase() === 'SIM';
    const idx  = s._idx;
    const data = s.Data ? String(s.Data).slice(0,10).split('-').reverse().join('/') : '—';
    const hora = s.Hora ? String(s.Hora).slice(0,5) : '—';
    const m10  = s.m10 != null ? s.m10.toFixed(3) : '—';
    const m15  = s.m15 != null ? s.m15.toFixed(3) : '—';
    const m5   = s.m5  != null ? s.m5.toFixed(3)  : '—';
    const lgc  = (s.LG_C != null && !isNaN(s.LG_C)) ? Math.round(s.LG_C) : '—';
    const hsc  = (s.H_Score_C != null && !isNaN(s.H_Score_C)) ? Math.round(s.H_Score_C) : '—';
    const emp  = s.Odd_Emp != null ? parseFloat(s.Odd_Emp).toFixed(1) : '—';
    return `<div class="card ${{apto?'apto':'napt'}}" onclick="openModal(${{idx}})">
      <div class="card-top">
        <span class="liga-tag">${{s.Liga||'?'}}</span>
        <span class="badge ${{apto?'apto':'napt'}}">${{apto?'&#x2714; APTO':'&#x2718;'}}</span>
        <span class="dt-tag">${{data}} ${{hora}}</span>
      </div>
      <div class="teams">${{s.Casa||'?'}} <span class="vs">&times;</span> ${{s.Visitante||'?'}}</div>
      <div class="probs">
        <span class="prob">m10: <b class="p10">${{m10}}</b></span>
        <span class="prob">m15: <b class="p15">${{m15}}</b></span>
        <span class="prob">m5: <b class="p5">${{m5}}</b></span>
      </div>
      <div class="sh-row">LG_C=${{lgc}} &middot; H=${{hsc}} &middot; Emp=${{emp}}</div>
    </div>`;
  }}).join('');
}}

function barColor(v) {{
  if (v >= 0.75) return 'var(--green)';
  if (v >= 0.60) return 'var(--blue)';
  if (v >= 0.50) return 'var(--yellow)';
  return 'var(--red)';
}}

function openModal(idx) {{
  const s = S[idx];
  const apto = String(s.APTO||'').toUpperCase() === 'SIM';
  const data = s.Data ? String(s.Data).slice(0,10).split('-').reverse().join('/') : '—';
  const hora = s.Hora ? String(s.Hora).slice(0,5) : '—';
  const lgc  = (s.LG_C  != null && !isNaN(s.LG_C))  ? Math.round(s.LG_C)  : '—';
  const lgv  = (s.LG_V  != null && !isNaN(s.LG_V))  ? Math.round(s.LG_V)  : '—';
  const hsc  = (s.H_Score_C != null && !isNaN(s.H_Score_C)) ? Math.round(s.H_Score_C) : '—';
  const oc   = s.Odd_Casa  != null ? parseFloat(s.Odd_Casa).toFixed(2)  : '—';
  const oe   = s.Odd_Emp   != null ? parseFloat(s.Odd_Emp).toFixed(2)   : '—';
  const ov   = s.Odd_Visit != null ? parseFloat(s.Odd_Visit).toFixed(2) : '—';

  document.getElementById('m-liga').textContent  = s.Liga || '?';
  document.getElementById('m-teams').textContent = (s.Casa||'?') + ' \u00d7 ' + (s.Visitante||'?');

  document.getElementById('m-entry').innerHTML = `
    <div class="erow"><span class="el">Data / Hora</span><span class="ev y">${{data}} ${{hora}}</span></div>
    <div class="erow"><span class="el">Mercado</span><span class="ev">Over 0.5 HT</span></div>
    <div class="erow"><span class="el">Entrada</span><span class="ev g">Minuto 10 (0×0 confirmado)</span></div>
    <div class="erow"><span class="el">Sa&iacute;da / Red</span><span class="ev">Minuto 35</span></div>
    <div class="erow"><span class="el">Odd Empate (ref)</span><span class="ev b">${{oe}}</span></div>
    <div class="erow"><span class="el">Prob min10</span><span class="ev g">${{s.m10 != null ? (s.m10*100).toFixed(1)+'%' : '—'}}</span></div>
    <div class="erow"><span class="el">Prob min15</span><span class="ev b">${{s.m15 != null ? (s.m15*100).toFixed(1)+'%' : '—'}}</span></div>`;

  document.getElementById('m-probs').innerHTML = [['m5',s.m5],['m10',s.m10],['m15',s.m15]].map(([lbl,v]) => {{
    if (v == null) return '';
    const pct = Math.min(100, v*100).toFixed(1);
    const c   = barColor(v);
    return `<div class="pbar-row">
      <span class="pbar-lbl">${{lbl}}</span>
      <div class="pbar-track"><div class="pbar-fill" style="width:${{pct}}%;background:${{c}}"></div></div>
      <span class="pbar-val" style="color:${{c}}">${{pct}}%</span></div>`;
  }}).join('');

  document.getElementById('m-sh').innerHTML = `
    <div class="iitem"><div class="ilbl">LG Score Casa</div><div class="ival">${{lgc}}</div></div>
    <div class="iitem"><div class="ilbl">LG Score Visit</div><div class="ival">${{lgv}}</div></div>
    <div class="iitem"><div class="ilbl">H Score Casa</div><div class="ival">${{hsc}}</div></div>
    <div class="iitem"><div class="ilbl">Odd Casa</div><div class="ival">${{oc}}</div></div>
    <div class="iitem"><div class="ilbl">Odd Empate</div><div class="ival">${{oe}}</div></div>
    <div class="iitem"><div class="ilbl">Odd Visitante</div><div class="ival">${{ov}}</div></div>`;

  const crits = [
    ['m10 &ge; 0.55', s.m10 != null && s.m10 >= 0.55, s.m10 != null ? s.m10.toFixed(3) : '—'],
    ['m15 &ge; 0.52', s.m15 != null && s.m15 >= 0.52, s.m15 != null ? s.m15.toFixed(3) : '—'],
    ['LG_C &ge; 100',  s.LG_C != null && s.LG_C >= 100, lgc],
    ['Odd_Emp &ge; 3.5', s.Odd_Emp != null && s.Odd_Emp >= 3.5, oe],
  ];
  document.getElementById('m-crits').innerHTML = crits.map(([lbl,ok,val]) =>
    `<div class="crit"><span class="ico">${{ok?'&#x2705;':'&#x274C;'}}</span><span class="clbl">${{lbl}}</span><span class="cval" style="color:${{ok?'var(--green)':'var(--red)}}">${{val}}</span></div>`
  ).join('');

  const ms = document.getElementById('m-motivo-sec');
  if (!apto && s.MOTIVO && s.MOTIVO !== 'OK') {{
    document.getElementById('m-motivo').textContent = s.MOTIVO;
    ms.style.display = 'block';
  }} else {{ ms.style.display = 'none'; }}

  document.getElementById('overlay').classList.add('open');
}}

function closeModal() {{ document.getElementById('overlay').classList.remove('open'); }}
function overlayClick(e) {{ if (e.target === document.getElementById('overlay')) closeModal(); }}
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') closeModal(); }});

render();
</script>
</body>
</html>"""


def main():
    PASTA_DOCS.mkdir(exist_ok=True)
    sinais  = carregar_sinais()
    payload = montar_json(sinais)
    with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    html = gerar_html(payload)
    with open(ARQUIVO_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Página gerada: {ARQUIVO_HTML}")
    print(f"JSON gerado:   {ARQUIVO_JSON}")
    print(f"APTOS: {payload['n_apto']} | Total: {payload['total']}")


if __name__ == "__main__":
    main()
