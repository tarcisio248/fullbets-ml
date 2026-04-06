"""
footystats_perfil_1gol.py — Perfil de Primeiro Gol por Equipe (FootyStats)
Gerado automaticamente a partir do notebook Jupyter.
"""

# # FootyStats — Perfil de Primeiro Gol por Equipe
# 
# Para cada time das 29 ligas, calcula:
# - **% de jogos** onde marcou o 1º gol (casa, fora, geral) — direto da API
# - **Minuto médio + DP + CV** em que marca/sofre o 1º gol — calculado dos timings reais
# - **% por faixa de 10min** em que marcou o 1º gol (casa e fora)
# 
# **Fontes:**
# - `league-teams` → `firstGoalScoredPercentage_home/away/overall`
# - `league-matches` → `homeGoals_timings` / `awayGoals_timings` (minutos exatos)

# ## 1. Imports

import time
import requests
import pandas as pd
import numpy as np

# ## 2. Configuração

API_KEY       = "033f6d5fc7b707b144fbd2c28898c1540eb066ee9efea6c038d740ffedeacb5f"
URL_TEAMS     = "https://api.football-data-api.com/league-teams"
URL_MATCHES   = "https://api.football-data-api.com/league-matches"
MAX_PER_PAGE  = 400
DELAY         = 1.0

# ## 3. Lista de Ligas

LIGAS = [
    {"liga": "Alemanha_1 Bundesliga",        "season_id": 14968},
    {"liga": "Alemanha_2 2. Bundesliga",     "season_id": 14931},
    {"liga": "Alemanha_3 3. Liga",           "season_id": 14977},
    {"liga": "Australia_1 A-League",         "season_id": 16036},
    {"liga": "Belgica_1 Pro League",         "season_id": 14937},
    {"liga": "Bulgaria_1 First League",      "season_id": 15056},
    {"liga": "Croacia_1 HNL",               "season_id": 15053},
    {"liga": "Dinamarca_1 Superliga",        "season_id": 15055},
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
print(f"Total de ligas: {len(LIGAS)}")

# ## 4. Funções de Busca

def buscar_teams(season_id: int, api_key: str) -> list:
    """Busca todos os times de uma liga com stats."""
    resp = requests.get(
        URL_TEAMS,
        params={"key": api_key, "season_id": season_id, "include": "stats"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", []) if data.get("success") else []


def buscar_todos_jogos(season_id: int, api_key: str) -> list:
    """Busca todos os jogos finalizados com timings registrados (todas as paginas)."""
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
        todos.extend([
            j for j in jogos
            if j.get("status") == "complete"
            and j.get("goal_timings_recorded") == 1
        ])
        if page >= max_page:
            break
        page += 1
        time.sleep(DELAY)
    return todos

# ## 5. Funcao de Calculo — Perfil de Primeiro Gol

# Faixas de 10 em 10 minutos
FAIXAS = [
    ("0_10",   0,  10),
    ("11_20", 11,  20),
    ("21_30", 21,  30),
    ("31_40", 31,  40),
    ("41_50", 41,  50),
    ("51_60", 51,  60),
    ("61_70", 61,  70),
    ("71_80", 71,  80),
    ("81_90", 81, 999),
]


def parse_timings(raw) -> list:
    """Converte lista de minutos (string) para inteiros. Trata acrescimos: '90+3' -> 93."""
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


def calcular_perfil_1gol(team_name: str, jogos: list) -> dict:
    """
    Para um time, percorre todos os jogos e coleta o minuto do 1o gol
    marcado e sofrido, separado por casa e fora.
    Calcula: media, DP, CV e % por faixa de 10 minutos.
    """
    marcou_1_casa, sofreu_1_casa = [], []
    marcou_1_fora, sofreu_1_fora = [], []

    for j in jogos:
        e_casa = j.get("home_name") == team_name
        e_fora = j.get("away_name") == team_name
        if not e_casa and not e_fora:
            continue

        if e_casa:
            meus   = parse_timings(j.get("homeGoals_timings") or j.get("homeGoals"))
            rivais = parse_timings(j.get("awayGoals_timings") or j.get("awayGoals"))
        else:
            meus   = parse_timings(j.get("awayGoals_timings") or j.get("awayGoals"))
            rivais = parse_timings(j.get("homeGoals_timings") or j.get("homeGoals"))

        todos = [(m, "meu") for m in meus] + [(m, "rival") for m in rivais]
        if not todos:
            continue  # 0x0

        primeiro_min, primeiro_de = min(todos, key=lambda x: x[0])

        if e_casa:
            (marcou_1_casa if primeiro_de == "meu" else sofreu_1_casa).append(primeiro_min)
        else:
            (marcou_1_fora if primeiro_de == "meu" else sofreu_1_fora).append(primeiro_min)

    # ── Funcoes estatisticas ─────────────────────────────────────────
    def media(lst):
        return round(float(np.mean(lst)), 1) if lst else np.nan

    def dp(lst):
        """Desvio Padrao amostral (ddof=1)."""
        return round(float(np.std(lst, ddof=1)), 1) if len(lst) >= 2 else np.nan

    def cv(lst):
        """Coeficiente de Variacao % = (DP / media) x 100."""
        if len(lst) < 2:
            return np.nan
        m = np.mean(lst)
        if m == 0:
            return np.nan
        return round(float(np.std(lst, ddof=1) / m * 100), 1)

    def pct_faixa(lst_minutos, total_jogos, low, high):
        """% dos jogos totais (marcou + sofreu) em que o 1o gol caiu nessa faixa."""
        if total_jogos == 0:
            return np.nan
        n = sum(1 for m in lst_minutos if low <= m <= high)
        return round(n / total_jogos * 100, 1)

    total_casa = len(marcou_1_casa) + len(sofreu_1_casa)
    total_fora = len(marcou_1_fora) + len(sofreu_1_fora)

    resultado = {
        # Contagens brutas
        "n_marcou_1gol_casa"       : len(marcou_1_casa),
        "n_sofreu_1gol_casa"       : len(sofreu_1_casa),
        "n_marcou_1gol_fora"       : len(marcou_1_fora),
        "n_sofreu_1gol_fora"       : len(sofreu_1_fora),
        # Media
        "avg_min_marcou_1gol_casa" : media(marcou_1_casa),
        "avg_min_marcou_1gol_fora" : media(marcou_1_fora),
        "avg_min_sofreu_1gol_casa" : media(sofreu_1_casa),
        "avg_min_sofreu_1gol_fora" : media(sofreu_1_fora),
        # Desvio Padrao
        "dp_min_marcou_1gol_casa"  : dp(marcou_1_casa),
        "dp_min_marcou_1gol_fora"  : dp(marcou_1_fora),
        "dp_min_sofreu_1gol_casa"  : dp(sofreu_1_casa),
        "dp_min_sofreu_1gol_fora"  : dp(sofreu_1_fora),
        # Coeficiente de Variacao %
        "cv_min_marcou_1gol_casa"  : cv(marcou_1_casa),
        "cv_min_marcou_1gol_fora"  : cv(marcou_1_fora),
        "cv_min_sofreu_1gol_casa"  : cv(sofreu_1_casa),
        "cv_min_sofreu_1gol_fora"  : cv(sofreu_1_fora),
    }

    # % por faixa de 10 em 10 minutos
    for label, low, high in FAIXAS:
        resultado[f"%Gl_Ca_{label}M"] = pct_faixa(marcou_1_casa, total_casa, low, high)
        resultado[f"%Gl_Fo_{label}M"] = pct_faixa(marcou_1_fora, total_fora, low, high)

    return resultado

# ## 6. Funcao Principal

def buscar_perfil_1gol_todas_ligas(
    api_key : str  = API_KEY,
    ligas   : list = LIGAS,
) -> pd.DataFrame:
    """
    Para cada liga:
      1. Busca stats dos times (% de 1o gol da API)
      2. Busca todos os jogos finalizados com timings
      3. Calcula perfil de 1o gol por time (media, DP, CV, % por faixa)
    Retorna DataFrame com uma linha por time.
    """
    frames = []
    erros  = []

    for i, entry in enumerate(ligas, 1):
        liga_nome = entry["liga"]
        season_id = entry["season_id"]

        print(f"[{i:02d}/{len(ligas)}] {liga_nome} (season_id={season_id})")

        try:
            # 1. Times + stats da API
            teams = buscar_teams(season_id, api_key)
            if not teams:
                print(f"  aviso: Sem times retornados.")
                continue
            time.sleep(DELAY)

            # 2. Jogos finalizados com timings
            jogos = buscar_todos_jogos(season_id, api_key)
            print(f"  -> {len(teams)} times | {len(jogos)} jogos c/ timings")

            # 3. Calcular perfil por time
            linhas = []
            for t in teams:
                stats     = t.get("stats", {})
                nome_time = t.get("cleanName") or t.get("name", "")

                perfil = calcular_perfil_1gol(nome_time, jogos)

                # Colunas fixas
                linha = {
                    "Liga"                     : liga_nome,
                    "time"                     : nome_time,
                    # % da API
                    "pct_marcou_1gol_casa"     : stats.get("firstGoalScoredPercentage_home"),
                    "pct_marcou_1gol_fora"     : stats.get("firstGoalScoredPercentage_away"),
                    "pct_marcou_1gol_geral"    : stats.get("firstGoalScoredPercentage_overall"),
                    # Media
                    "avg_min_marcou_1gol_casa" : perfil["avg_min_marcou_1gol_casa"],
                    "avg_min_marcou_1gol_fora" : perfil["avg_min_marcou_1gol_fora"],
                    "avg_min_sofreu_1gol_casa" : perfil["avg_min_sofreu_1gol_casa"],
                    "avg_min_sofreu_1gol_fora" : perfil["avg_min_sofreu_1gol_fora"],
                    # Desvio Padrao
                    "dp_min_marcou_1gol_casa"  : perfil["dp_min_marcou_1gol_casa"],
                    "dp_min_marcou_1gol_fora"  : perfil["dp_min_marcou_1gol_fora"],
                    "dp_min_sofreu_1gol_casa"  : perfil["dp_min_sofreu_1gol_casa"],
                    "dp_min_sofreu_1gol_fora"  : perfil["dp_min_sofreu_1gol_fora"],
                    # Coeficiente de Variacao %
                    "cv_min_marcou_1gol_casa"  : perfil["cv_min_marcou_1gol_casa"],
                    "cv_min_marcou_1gol_fora"  : perfil["cv_min_marcou_1gol_fora"],
                    "cv_min_sofreu_1gol_casa"  : perfil["cv_min_sofreu_1gol_casa"],
                    "cv_min_sofreu_1gol_fora"  : perfil["cv_min_sofreu_1gol_fora"],
                    # Contagens brutas
                    "n_marcou_1gol_casa"       : perfil["n_marcou_1gol_casa"],
                    "n_sofreu_1gol_casa"       : perfil["n_sofreu_1gol_casa"],
                    "n_marcou_1gol_fora"       : perfil["n_marcou_1gol_fora"],
                    "n_sofreu_1gol_fora"       : perfil["n_sofreu_1gol_fora"],
                }

                # Colunas dinamicas %Gl_*
                for k, v in perfil.items():
                    if k.startswith("%Gl_"):
                        linha[k] = v

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

    print(f"\n{'-'*55}")
    print(f"Ligas OK      : {len(frames)}/{len(ligas)}")
    print(f"Ligas c/ erro : {len(erros)}")
    if erros:
        for e in erros:
            print(f"  - {e['liga']}: {e['erro']}")

    if not frames:
        return pd.DataFrame()

    df_final = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["Liga", "time"])
        .reset_index(drop=True)
    )
    print(f"\nDataFrame final: {df_final.shape[0]} times x {df_final.shape[1]} colunas")
    print(f"Colunas: {df_final.columns.tolist()}")
    return df_final

# ## 7. Execucao

df_1gol = buscar_perfil_1gol_todas_ligas()

# ## 8. Visualizacao

import warnings
warnings.filterwarnings('ignore')
pd.options.mode.copy_on_write = True 
pd.set_option('display.max_columns', None)

def drop_reset_index(df):
    df = df.dropna()
    df = df.reset_index(drop=True)
    df.index += 1
    return df

df_1gol.to_csv("0_PAINEL_EVOLUTION_JOGOS_MOMENTO_1GOL_API.csv", index=False)

df_1gol.head(5)

# Times que mais marcam primeiro em casa com dispersao
df_1gol.nlargest(15, "pct_marcou_1gol_casa")[
    ["Liga", "time", "pct_marcou_1gol_casa",
     "avg_min_marcou_1gol_casa", "dp_min_marcou_1gol_casa", "cv_min_marcou_1gol_casa"]
]

# Times que marcam o 1o gol mais cedo jogando fora (menor avg + menor DP = mais previsivel)
df_1gol.dropna(subset=["avg_min_marcou_1gol_fora"]).nsmallest(15, "avg_min_marcou_1gol_fora")[
    ["Liga", "time", "pct_marcou_1gol_fora",
     "avg_min_marcou_1gol_fora", "dp_min_marcou_1gol_fora", "cv_min_marcou_1gol_fora"]
]

sorted(df_1gol["Liga"].dropna().unique().tolist())

# Filtrar por liga e ver distribuicao por faixa de 10min
liga_filtro = "Inglaterra_1 Premier League"
cols_faixa_casa = [c for c in df_1gol.columns if c.startswith("%Gl_Ca_")]

df_1gol[df_1gol["Liga"] == liga_filtro][[
    "time", "pct_marcou_1gol_casa",
    "avg_min_marcou_1gol_casa", "dp_min_marcou_1gol_casa", "cv_min_marcou_1gol_casa"
] + cols_faixa_casa].sort_values("pct_marcou_1gol_casa", ascending=False)

