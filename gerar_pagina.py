"""
gerar_pagina.py — Gera docs/index.html e docs/sinais.json
para publicação na GitHub Page (tarcisio248.github.io/fullbets-ml)

Lê sinais_hoje.xlsx gerado pelo Módulo C do pipeline e cria
uma página HTML responsiva com os sinais do dia.
"""

import json
import os
from datetime import datetime, date

import pandas as pd

ARQUIVO_SINAIS = "sinais_hoje.xlsx"
DOCS_DIR       = "docs"
HTML_OUT       = os.path.join(DOCS_DIR, "index.html")
JSON_OUT       = os.path.join(DOCS_DIR, "sinais.json")

os.makedirs(DOCS_DIR, exist_ok=True)

# ── Carregar sinais ───────────────────────────────────────────────
if os.path.exists(ARQUIVO_SINAIS):
    df = pd.read_excel(ARQUIVO_SINAIS)
    sinais = df.to_dict("records")
else:
    sinais = []

# Timestamp
agora = datetime.now().strftime("%d/%m/%Y %H:%M")
hoje  = date.today().strftime("%d/%m/%Y")

# ── Salvar JSON para uso externo ──────────────────────────────────
with open(JSON_OUT, "w", encoding="utf-8") as f:
    json.dump({"updated": agora, "sinais": sinais}, f,
              ensure_ascii=False, default=str)

# ── Separar por status e janela ───────────────────────────────────
watch  = [s for s in sinais if s.get("status") == "WATCH"]
skip   = [s for s in sinais if s.get("status") == "SKIP"]
alertas = [s for s in sinais if s.get("status") == "ALERTA"]

def badge_prob(p):
    if p is None: return "—"
    p = float(p)
    if p >= 0.65: cor = "#0F6E56"
    elif p >= 0.55: cor = "#1D9E75"
    elif p >= 0.50: cor = "#BA7517"
    else: cor = "#888780"
    return f'<span style="background:{cor};color:#fff;padding:2px 8px;border-radius:8px;font-size:12px;font-weight:500">{p:.3f}</span>'

def card_jogo(s):
    p10 = badge_prob(s.get("prob_m10"))
    p15 = badge_prob(s.get("prob_m15"))
    lg  = s.get("LG_C", "—")
    hs  = s.get("H_Score_C", "—")
    oe  = s.get("Odd_Emp", "—")
    st  = s.get("status", "")
    borda = "#1D9E75" if st == "WATCH" else ("#534AB7" if st == "ALERTA" else "#D3D1C7")
    return f"""
    <div style="border:1px solid {borda};border-radius:10px;padding:12px 16px;margin-bottom:10px;background:#fff">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
        <span style="font-size:12px;color:#888;font-weight:500">{s.get('Liga','')}</span>
        <span style="font-size:11px;color:#aaa">{s.get('Hora','')}</span>
      </div>
      <div style="font-size:15px;font-weight:600;color:#222;margin-bottom:8px">
        {s.get('Casa','')} × {s.get('Visitante','')}
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;font-size:12px">
        <span>m10: {p10}</span>
        <span>m15: {p15}</span>
        <span style="color:#666">LG={lg} · H={hs} · Emp={oe}</span>
      </div>
    </div>"""

cards_watch  = "".join(card_jogo(s) for s in watch)  if watch  else '<p style="color:#aaa">Nenhum jogo em WATCH hoje.</p>'
cards_alerta = "".join(card_jogo(s) for s in alertas) if alertas else '<p style="color:#aaa">Nenhum alerta live ainda.</p>'

# ── Gerar HTML ────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FULLBETS ML — Sinais do Dia</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0 }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f4f4f4; color: #222; padding: 16px }}
    .header {{ background: #1a1a2e; color: #fff; border-radius: 12px;
               padding: 20px; margin-bottom: 20px; text-align: center }}
    .header h1 {{ font-size: 22px; margin-bottom: 4px }}
    .header p  {{ font-size: 13px; color: #aaa }}
    .section {{ background: #fff; border-radius: 10px; padding: 16px;
                margin-bottom: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.08) }}
    .section h2 {{ font-size: 15px; font-weight: 600; margin-bottom: 12px;
                  display: flex; align-items: center; gap: 8px }}
    .badge {{ font-size: 11px; padding: 2px 8px; border-radius: 8px;
              font-weight: 500 }}
    .stats {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 10px;
              margin-bottom: 16px }}
    .stat {{ background: #f8f8f8; border-radius: 8px; padding: 12px; text-align: center }}
    .stat .n {{ font-size: 26px; font-weight: 700; color: #1a1a2e }}
    .stat .l {{ font-size: 11px; color: #888; margin-top: 2px }}
    .footer {{ text-align: center; font-size: 11px; color: #aaa; margin-top: 20px }}
  </style>
</head>
<body>

<div class="header">
  <h1>FULLBETS ML</h1>
  <p>Sinais Over 0.5 HT — {hoje}</p>
  <p style="font-size:11px;margin-top:4px">Atualizado: {agora} | Modelo min10 th≥0.55</p>
</div>

<div class="stats">
  <div class="stat"><div class="n">{len(sinais)}</div><div class="l">jogos analisados</div></div>
  <div class="stat"><div class="n" style="color:#1D9E75">{len(watch)}</div><div class="l">em WATCH</div></div>
  <div class="stat"><div class="n" style="color:#534AB7">{len(alertas)}</div><div class="l">alertas live</div></div>
</div>

<div class="section">
  <h2>
    <span class="badge" style="background:#E1F5EE;color:#0F6E56">WATCH</span>
    Jogos pré-selecionados ({len(watch)})
  </h2>
  {cards_watch}
</div>

<div class="section">
  <h2>
    <span class="badge" style="background:#EEEDFE;color:#3C3489">ALERTA LIVE</span>
    Entradas confirmadas ({len(alertas)})
  </h2>
  {cards_alerta}
</div>

<div class="footer">
  <p>FULLBETS ML · tarcisio248 · {date.today().year}</p>
  <p>Entrada min10 · Saída min35 · ROI histórico +14% · 13/13 semanas positivas</p>
</div>

</body>
</html>"""

with open(HTML_OUT, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Página gerada: {HTML_OUT}")
print(f"JSON gerado:   {JSON_OUT}")
print(f"Watch: {len(watch)} | Alertas: {len(alertas)} | Total: {len(sinais)}")
