"""
gerar_pagina.py — FULLBETS
==========================
Lê sinais_hoje.xlsx e gera docs/index.html + docs/sinais.json
para publicação no GitHub Pages.

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

# ─────────────────────────────────────────────────────────────────────
def carregar_sinais() -> list:
    if not os.path.exists(ARQUIVO_SINAIS):
        return []
    df = pd.read_excel(ARQUIVO_SINAIS)
    df = df.where(pd.notnull(df), None)
    return df.to_dict("records")


def montar_json(sinais: list) -> dict:
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    apto  = [s for s in sinais if str(s.get("APTO", "")).upper() == "SIM"]
    return {
        "atualizado" : agora,
        "total"      : len(sinais),
        "n_apto"     : len(apto),
        "sinais"     : sinais,
    }


def gerar_html(payload: dict) -> str:
    agora   = payload["atualizado"]
    total   = payload["total"]
    n_apto  = payload["n_apto"]
    sinais  = payload["sinais"]
    json_str = json.dumps(payload, ensure_ascii=False, default=str)

    # Separar APTO / NÃO APTO
    apto_list = [s for s in sinais if str(s.get("APTO","")).upper() == "SIM"]
    napt_list = [s for s in sinais if str(s.get("APTO","")).upper() != "SIM"]

    def card(s):
        m10   = s.get("m10")
        m15   = s.get("m15")
        m5    = s.get("m5")
        lg_c  = s.get("LG_C","—")
        h_c   = s.get("H_Score_C","—")
        odd_e = s.get("Odd_Emp","—")
        motiv = s.get("MOTIVO","")
        apto  = str(s.get("APTO","")).upper() == "SIM"
        hora  = str(s.get("Hora",""))[:5] if s.get("Hora") else "—"

        m10_str = f"{m10:.3f}" if m10 is not None else "—"
        m15_str = f"{m15:.3f}" if m15 is not None else "—"
        m5_str  = f"{m5:.3f}"  if m5  is not None else "—"
        try:
            lg_str = f"{int(float(lg_c))}" if lg_c is not None and str(lg_c) != "nan" else "—"
        except (ValueError, TypeError):
            lg_str = "—"
        try:
            hc_str = f"{int(float(h_c))}" if h_c is not None and str(h_c) != "nan" else "—"
        except (ValueError, TypeError):
            hc_str = "—"
        oe_str  = f"{float(odd_e):.1f}" if odd_e is not None else "—"

        badge = '<span class="badge apto">✔ APTO</span>' if apto \
               else '<span class="badge napt">✘</span>'

        motivo_html = f'<div class="motivo">{motiv}</div>' if not apto and motiv and motiv != "OK" else ""

        return f"""
        <div class="card {'card-apto' if apto else 'card-napt'}">
          <div class="card-header">
            <span class="liga">{s.get('Liga','?')}</span>
            {badge}
            <span class="hora">{hora}</span>
          </div>
          <div class="teams">{s.get('Casa','?')} <span class="vs">×</span> {s.get('Visitante','?')}</div>
          <div class="metrics">
            <span class="m m10">m10: <b>{m10_str}</b></span>
            <span class="m m15">m15: <b>{m15_str}</b></span>
            <span class="m m5">m5: <b>{m5_str}</b></span>
          </div>
          <div class="sh">LG_C={lg_str} · H={hc_str} · Emp={oe_str}</div>
          {motivo_html}
        </div>"""

    cards_apto = "\n".join(card(s) for s in apto_list) or "<p class='vazio'>Nenhum jogo APTO hoje.</p>"
    cards_napt = "\n".join(card(s) for s in napt_list) or "<p class='vazio'>—</p>"

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FULLBETS ML — Sinais do Dia</title>
<style>
  :root {{
    --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
    --green: #3fb950; --red: #f85149; --blue: #58a6ff;
    --text: #c9d1d9; --muted: #8b949e; --border: #30363d;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: -apple-system, sans-serif; }}
  header {{ background: var(--bg2); border-bottom: 1px solid var(--border); padding: 20px; text-align: center; }}
  header h1 {{ font-size: 1.5rem; color: var(--blue); letter-spacing: 2px; }}
  header p  {{ color: var(--muted); font-size: .85rem; margin-top: 4px; }}
  .kpis {{ display: flex; gap: 1px; background: var(--border); border-bottom: 1px solid var(--border); }}
  .kpi  {{ flex: 1; background: var(--bg2); padding: 16px; text-align: center; }}
  .kpi .val {{ font-size: 2rem; font-weight: 700; }}
  .kpi .lbl {{ color: var(--muted); font-size: .8rem; margin-top: 2px; }}
  .val.green {{ color: var(--green); }}
  .val.blue  {{ color: var(--blue); }}
  .section {{ padding: 20px; max-width: 900px; margin: 0 auto; }}
  .section-title {{ font-size: .75rem; font-weight: 600; letter-spacing: 1px;
                    color: var(--muted); text-transform: uppercase;
                    border-bottom: 1px solid var(--border); padding-bottom: 8px; margin-bottom: 12px; }}
  .cards {{ display: grid; gap: 10px; }}
  .card {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }}
  .card-apto {{ border-left: 3px solid var(--green); }}
  .card-napt {{ border-left: 3px solid var(--border); opacity: .75; }}
  .card-header {{ display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }}
  .liga {{ font-size: .72rem; color: var(--muted); flex: 1; }}
  .hora {{ font-size: .72rem; color: var(--muted); }}
  .badge {{ font-size: .7rem; font-weight: 600; padding: 2px 8px; border-radius: 10px; }}
  .badge.apto {{ background: rgba(63,185,80,.15); color: var(--green); }}
  .badge.napt {{ background: rgba(248,81,73,.10); color: var(--red); }}
  .teams {{ font-size: 1rem; font-weight: 500; margin-bottom: 8px; }}
  .vs {{ color: var(--muted); margin: 0 6px; }}
  .metrics {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 5px; }}
  .m {{ font-size: .82rem; }}
  .m10 b {{ color: var(--blue); }}
  .m15 b {{ color: var(--green); }}
  .m5  b {{ color: var(--muted); }}
  .sh {{ font-size: .75rem; color: var(--muted); }}
  .motivo {{ font-size: .72rem; color: var(--red); margin-top: 5px; }}
  .vazio {{ color: var(--muted); font-size: .85rem; padding: 10px 0; }}
  footer {{ text-align: center; color: var(--muted); font-size: .75rem; padding: 24px; border-top: 1px solid var(--border); }}
</style>
</head>
<body>
<header>
  <h1>FULLBETS ML</h1>
  <p>Sinais Over 0.5 HT — {datetime.now().strftime('%d/%m/%Y')}</p>
  <p>Atualizado: {agora} | Critérios: m10≥0.55 · m15≥0.52 · LG_C≥100 · Odd_Emp≥3.5</p>
</header>

<div class="kpis">
  <div class="kpi"><div class="val blue">{total}</div><div class="lbl">jogos analisados</div></div>
  <div class="kpi"><div class="val green">{n_apto}</div><div class="lbl">APTOS para entrada</div></div>
</div>

<div class="section">
  <div class="section-title">✔ Jogos APTOS para entrada ({n_apto})</div>
  <div class="cards">
    {cards_apto}
  </div>
</div>

<div class="section">
  <div class="section-title">Demais jogos analisados ({len(napt_list)})</div>
  <div class="cards">
    {cards_napt}
  </div>
</div>

<footer>
  FULLBETS ML · tarcisio248 · 2026<br>
  Entrada min10 · Saída min35 · ROI histórico +14% · 13/13 semanas positivas<br>
  <small>Decisão final de entrada é sempre manual.</small>
</footer>

<script>
const DADOS = {json_str};
</script>
</body>
</html>"""


def main():
    PASTA_DOCS.mkdir(exist_ok=True)

    sinais  = carregar_sinais()
    payload = montar_json(sinais)

    # JSON
    with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    # HTML
    html = gerar_html(payload)
    with open(ARQUIVO_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    n_apto = payload["n_apto"]
    print(f"Página gerada: {ARQUIVO_HTML}")
    print(f"JSON gerado:   {ARQUIVO_JSON}")
    print(f"APTOS: {n_apto} | Total: {payload['total']}")


if __name__ == "__main__":
    main()
