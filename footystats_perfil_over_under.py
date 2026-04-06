"""
footystats_perfil_over_under.py — Perfil Over/Under HT + 2H por Equipe (FootyStats)
Gerado automaticamente a partir do notebook Jupyter.
"""

# # FootyStats — Perfil Over/Under HT + 2H + Barra de Intensidade por Tempo
# 
# Gera **um CSV único** com, por time e contexto (casa/fora):
# 
# ### Tabela Over/Under (para exibir no dashboard)
# - `over05_ht_casa/fora` — % jogos com >0.5 gols no 1º tempo
# - `over15_ht_casa/fora` — % jogos com >1.5 gols no 1º tempo
# - `over25_ht_casa/fora` — % jogos com >2.5 gols no 1º tempo
# - `over05_2h_casa/fora` — % jogos com >0.5 gols no 2º tempo
# - `over15_2h_casa/fora` — % jogos com >1.5 gols no 2º tempo
# - `over25_2h_casa/fora` — % jogos com >2.5 gols no 2º tempo
# - `media_gols_ht_casa/fora` — média de gols no 1º tempo
# - `media_gols_2h_casa/fora` — média de gols no 2º tempo
# 
# ### Barra de intensidade por janela de 10min (para barra colorida)
# - `%Gl_Ca_0_10M` ... `%Gl_Ca_41_50M` — % jogos com gol nessa janela (casa)
# - `%Gl_Fo_0_10M` ... `%Gl_Fo_41_50M` — % jogos com gol nessa janela (fora)
# - `%Gl_Ca_61_70M`, `71_80M`, `81_90M` — janelas 2º tempo (casa)
# - `%Gl_Fo_61_70M`, `71_80M`, `81_90M` — janelas 2º tempo (fora)
# - `%Gl_Ca_51_60M` / `%Gl_Fo_51_60M` — janela 51-60min (transição HT→2H)
# 
# **Arquivo gerado:** `PAINEL_PERFIL_OVER_UNDER.csv`

# ## 1. Imports

import time
import requests
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
pd.options.mode.copy_on_write = True
pd.set_option('display.max_columns', None)
print('OK')

# ## 2. Configuração

API_KEY      = "033f6d5fc7b707b144fbd2c28898c1540eb066ee9efea6c038d740ffedeacb5f"
URL_MATCHES  = "https://api.football-data-api.com/league-matches"
MAX_PER_PAGE = 400
DELAY        = 1.0

# Janelas de 10min para a barra colorida
# HT: 0-50min (incluindo 51-60 como zona de transição)
# 2H: 61-90min
FAIXAS_HT = [
    ("0_10",   0,  10),
    ("11_20", 11,  20),
    ("21_30", 21,  30),
    ("31_40", 31,  40),
    ("41_50", 41,  50),
    ("51_60", 51,  60),  # zona de transição / acréscimos HT
]
FAIXAS_2H = [
    ("61_70", 61,  70),
    ("71_80", 71,  80),
    ("81_90", 81, 999),
]
TODAS_FAIXAS = FAIXAS_HT + FAIXAS_2H
print(f'Faixas configuradas: {len(TODAS_FAIXAS)}')

# ## 3. Lista de Ligas

LIGAS = [
    {"liga": "Alemanha_1 Bundesliga",        "season_id": 14968},
    {"liga": "Alemanha_2 2. Bundesliga",     "season_id": 14931},
    {"liga": "Alemanha_3 3. Liga",           "season_id": 14977},
    {"liga": "Australia_1 A-League",         "season_id": 16036},
    {"liga": "Belgica_1 Pro League",         "season_id": 14937},
    {"liga": "Bulgaria_1 First League",      "season_id": 15056},
    {"liga": "Croacia_1 HNL",               "season_id": 15053},
    # {"liga": "Dinamarca_1 Superliga",        "season_id": 15055},
    {"liga": "Escocia_1 Premiership",        "season_id": 15000},
    {"liga": "Espanha_1 La Liga",            "season_id": 14956},
    {"liga": "Espanha_2 Segunda Division",   "season_id": 15066},
    {"liga": "Franca_1 Ligue 1",            "season_id": 14932},
    {"liga": "Franca_2 Ligue 2",            "season_id": 14954},
    {"liga": "Grecia_1 Super League",        "season_id": 15163},
    {"liga": "Holanda_1 Eredivisie",         "season_id": 14936},
    {"liga": "Holanda_2 Eerste Divisie",     "season_id": 14987},
    {"liga": "Hungria_1 NB I",              "season_id": 14963},
    {"liga": "Inglaterra_1 Premier League",  "season_id": 15050},
    {"liga": "Inglaterra_2 Championship",    "season_id": 14930},
    {"liga": "Inglaterra_3 League One",      "season_id": 14934},
    {"liga": "Italia_1 Serie A",             "season_id": 15068},
    {"liga": "Italia_2 Serie B",             "season_id": 15632},
    {"liga": "Portugal_1 Primeira Liga",     "season_id": 15115},
    {"liga": "Rep_Tcheca_1 Czech Liga",      "season_id": 14973},
    {"liga": "Turquia_1 Super Lig",          "season_id": 14972},
    {"liga": "Argentina_1 Liga Profesional", "season_id": 16571},
    {"liga": "Brasileiro_A Serie A",         "season_id": 16544},
    {"liga": "Chile_1 Primera Division",     "season_id": 16615},
    {"liga": "Colombia_1 Liga BetPlay",      "season_id": 16614},
    {"liga": "Suecia_1 Allsvenskan",         "season_id":16576},
    {"liga": "Suecia_2 Superettan",          "season_id":16575},
    {"liga": "Noruega_1 Eliteserien",        "season_id":16558},
    {"liga": "Noruega_2 First Division",     "season_id":16560},
    {"liga": "Brasileiro_B Serie B",         "season_id": 16783},
]
print(f'Total de ligas: {len(LIGAS)}')

# ## 4. Função de Busca (igual ao perfil M60)

def buscar_todos_jogos(season_id: int, api_key: str) -> list:
    """
    Busca TODOS os jogos finalizados da liga (todas as páginas).
    Inclui jogos 0x0 — necessário para denominador correto.
    """
    todos = []
    page  = 1
    while True:
        resp = requests.get(
            URL_MATCHES,
            params={"key": api_key, "season_id": season_id,
                    "max_per_page": MAX_PER_PAGE, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            break
        jogos    = data.get("data", [])
        pager    = data.get("pager", {})
        max_page = pager.get("max_page", 1)
        todos.extend([j for j in jogos if j.get("status") == "complete"])
        if page >= max_page:
            break
        page += 1
        time.sleep(DELAY)
    return todos

# ## 5. Funções de Parse e Cálculo

def parse_timings(raw) -> list:
    """Converte lista de minutos para inteiros. Trata acréscimos: '90+3' → 93."""
    if not raw:
        return []
    result = []
    for v in raw:
        try:
            s = str(v)
            if '+' in s:
                parts = s.split('+')
                result.append(int(parts[0]) + int(parts[1]))
            else:
                result.append(int(s))
        except:
            pass
    return result


def pct_faixa_gols(lista_todos_minutos: list, total_jogos: int, low: int, high: int) -> float:
    """% de eventos de gol na faixa [low, high] sobre total de jogos."""
    if total_jogos == 0:
        return float('nan')
    n = sum(1 for m in lista_todos_minutos if low <= m <= high)
    return round(n / total_jogos * 100, 1)


def calcular_perfil_over_under(team_name: str, jogos: list) -> dict:
    """
    Calcula Over/Under HT e 2H via campos diretos da API FootyStats:
      ht_goals_team_a / ht_goals_team_b  (gols HT)
      goals_2hg_team_a / goals_2hg_team_b (gols 2H)
    E barra de intensidade via homeGoals_timings / awayGoals_timings.
    """
    over_ht = {"05_ca": 0, "15_ca": 0, "25_ca": 0,
               "05_fo": 0, "15_fo": 0, "25_fo": 0}
    over_2h = {"05_ca": 0, "15_ca": 0, "25_ca": 0,
               "05_fo": 0, "15_fo": 0, "25_fo": 0}

    soma_ht_ca = 0; soma_ht_fo = 0
    soma_2h_ca = 0; soma_2h_fo = 0

    mins_ca = []
    mins_fo = []

    total_ca = 0; total_fo = 0
    total_ca_ht = 0; total_fo_ht = 0

    for j in jogos:
        e_casa = j.get("home_name") == team_name
        e_fora = j.get("away_name") == team_name
        if not e_casa and not e_fora:
            continue

        # ── Timings para barra de intensidade ──────────────────────────
        if e_casa:
            total_ca += 1
            mins_ca.extend(parse_timings(
                j.get("homeGoals_timings") or j.get("homeGoals") or []
            ))
        else:
            total_fo += 1
            mins_fo.extend(parse_timings(
                j.get("awayGoals_timings") or j.get("awayGoals") or []
            ))

        # ── Gols HT e 2H via campos diretos da API ──────────────────────
        # Campos reais confirmados: ht_goals_team_a (casa), ht_goals_team_b (fora)
        # goals_2hg_team_a (casa 2H), goals_2hg_team_b (fora 2H)
        try:
            ht_ca = int(j["ht_goals_team_a"])
            ht_fo = int(j["ht_goals_team_b"])
            s2h_ca = int(j["goals_2hg_team_a"])
            s2h_fo = int(j["goals_2hg_team_b"])
        except (KeyError, TypeError, ValueError):
            continue  # jogo sem dados de HT

        gols_ht = ht_ca + ht_fo       # total gols 1º tempo
        gols_2h = s2h_ca + s2h_fo     # total gols 2º tempo

        if e_casa:
            total_ca_ht += 1
            if gols_ht > 0.5: over_ht["05_ca"] += 1
            if gols_ht > 1.5: over_ht["15_ca"] += 1
            if gols_ht > 2.5: over_ht["25_ca"] += 1
            if gols_2h > 0.5: over_2h["05_ca"] += 1
            if gols_2h > 1.5: over_2h["15_ca"] += 1
            if gols_2h > 2.5: over_2h["25_ca"] += 1
            soma_ht_ca += gols_ht
            soma_2h_ca += gols_2h
        else:
            total_fo_ht += 1
            if gols_ht > 0.5: over_ht["05_fo"] += 1
            if gols_ht > 1.5: over_ht["15_fo"] += 1
            if gols_ht > 2.5: over_ht["25_fo"] += 1
            if gols_2h > 0.5: over_2h["05_fo"] += 1
            if gols_2h > 1.5: over_2h["15_fo"] += 1
            if gols_2h > 2.5: over_2h["25_fo"] += 1
            soma_ht_fo += gols_ht
            soma_2h_fo += gols_2h

    def p(n, tot): return round(n / tot * 100, 1) if tot > 0 else float('nan')
    def m(s, tot): return round(s / tot, 2)        if tot > 0 else float('nan')

    resultado = {
        "total_jogos_ca":    total_ca,
        "total_jogos_fo":    total_fo,
        "total_jogos_ht_ca": total_ca_ht,
        "total_jogos_ht_fo": total_fo_ht,
        # Over/Under HT
        "over05_ht_casa": p(over_ht["05_ca"], total_ca_ht),
        "over15_ht_casa": p(over_ht["15_ca"], total_ca_ht),
        "over25_ht_casa": p(over_ht["25_ca"], total_ca_ht),
        "over05_ht_fora": p(over_ht["05_fo"], total_fo_ht),
        "over15_ht_fora": p(over_ht["15_fo"], total_fo_ht),
        "over25_ht_fora": p(over_ht["25_fo"], total_fo_ht),
        # Over/Under 2H
        "over05_2h_casa": p(over_2h["05_ca"], total_ca_ht),
        "over15_2h_casa": p(over_2h["15_ca"], total_ca_ht),
        "over25_2h_casa": p(over_2h["25_ca"], total_ca_ht),
        "over05_2h_fora": p(over_2h["05_fo"], total_fo_ht),
        "over15_2h_fora": p(over_2h["15_fo"], total_fo_ht),
        "over25_2h_fora": p(over_2h["25_fo"], total_fo_ht),
        # Médias
        "media_gols_ht_casa": m(soma_ht_ca, total_ca_ht),
        "media_gols_ht_fora": m(soma_ht_fo, total_fo_ht),
        "media_gols_2h_casa": m(soma_2h_ca, total_ca_ht),
        "media_gols_2h_fora": m(soma_2h_fo, total_fo_ht),
    }

    for label, low, high in TODAS_FAIXAS:
        resultado[f"%Gl_Ca_{label}M"] = pct_faixa_gols(mins_ca, total_ca, low, high)
        resultado[f"%Gl_Fo_{label}M"] = pct_faixa_gols(mins_fo, total_fo, low, high)

    return resultado

print("Funções definidas OK")

# ## 6. Função Principal

def buscar_perfil_over_under_todas_ligas(
    api_key : str  = API_KEY,
    ligas   : list = LIGAS,
) -> pd.DataFrame:
    frames = []
    erros  = []

    for i, entry in enumerate(ligas, 1):
        liga_nome = entry["liga"]
        season_id = entry["season_id"]
        print(f"[{i:02d}/{len(ligas)}] {liga_nome} (season_id={season_id})")

        try:
            jogos = buscar_todos_jogos(season_id, api_key)
            times = sorted(set(
                [j["home_name"] for j in jogos if j.get("home_name")] +
                [j["away_name"] for j in jogos if j.get("away_name")]
            ))
            print(f"  -> {len(jogos)} jogos | {len(times)} times")

            linhas = []
            for nome in times:
                perfil = calcular_perfil_over_under(nome, jogos)
                linha  = {"Liga": liga_nome, "time": nome}
                linha.update(perfil)
                linhas.append(linha)

            df_liga = pd.DataFrame(linhas)
            frames.append(df_liga)
            print(f"  OK: {len(linhas)} times")

        except requests.exceptions.HTTPError as e:
            print(f"  ERRO HTTP {e.response.status_code}: {e}")
            erros.append({"liga": liga_nome, "erro": str(e)})
        except Exception as e:
            print(f"  ERRO: {e}")
            erros.append({"liga": liga_nome, "erro": str(e)})

        time.sleep(DELAY)

    print(f"\n{'─'*55}")
    print(f"Ligas OK      : {len(frames)}/{len(ligas)}")
    print(f"Ligas c/ erro : {len(erros)}")
    if erros:
        for e in erros:
            print(f"  - {e['liga']}: {e['erro']}")
    if not frames:
        return pd.DataFrame()

    df = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["Liga", "time"])
        .reset_index(drop=True)
    )
    print(f"\nDataFrame final: {df.shape[0]} times x {df.shape[1]} colunas")
    print(f"Colunas geradas: {df.columns.tolist()}")
    return df

# ## 7. Execução

df_ou = buscar_perfil_over_under_todas_ligas()

# ## 8. Salvar CSV

NOME_CSV = "0_PAINEL_PERFIL_OVER_UNDER.csv"
df_ou.to_csv(NOME_CSV, index=False)
print(f"Salvo: {NOME_CSV}")
print(f"Shape: {df_ou.shape}")
df_ou.head(3)

# ## 9. Visualizações de Validação

# Top 15 Over 0.5 HT jogando em casa
print('=== TOP 15 — Over 0.5 HT (casa) ===')
df_ou.nlargest(15, 'over05_ht_casa')[[
    'Liga', 'time',
    'over05_ht_casa', 'over15_ht_casa', 'over25_ht_casa',
    'media_gols_ht_casa'
]]

# Top 15 Over 0.5 2H jogando fora
print('=== TOP 15 — Over 0.5 2H (fora) ===')
df_ou.nlargest(15, 'over05_2h_fora')[[
    'Liga', 'time',
    'over05_2h_fora', 'over15_2h_fora', 'over25_2h_fora',
    'media_gols_2h_fora'
]]

# Validar barra de intensidade — exemplo: Premier League
liga_teste  = "Inglaterra_1 Premier League"
time_teste  = df_ou[df_ou['Liga'] == liga_teste]['time'].iloc[0]
colunas_bar = [c for c in df_ou.columns if c.startswith('%Gl_Ca_') or c.startswith('%Gl_Fo_')]
print(f'Barra de intensidade — {time_teste} ({liga_teste})')
print(df_ou[df_ou['time'] == time_teste][['time'] + colunas_bar].T)

# Resumo geral das colunas geradas
print('=== COLUNAS DO CSV ===')
for i, col in enumerate(df_ou.columns, 1):
    print(f'  {i:02d}. {col}')
print(f'\nTotal: {len(df_ou.columns)} colunas, {len(df_ou)} linhas')

