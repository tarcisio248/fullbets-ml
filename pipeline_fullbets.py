"""
pipeline_fullbets.py — FULLBETS Over 0.5 HT Pipeline Operacional
=================================================================
4 módulos em sequência única — rodar toda manhã:

  A. Coleta histórica    → resultados de ontem → alimenta base xlsx
  B. Treino do modelo    → GBM atualizado      → salva model.pkl
  C. Scanner pré-live    → jogos de hoje/amanhã → sinais_hoje.xlsx
  D. Monitor live min5   → watchlist do scanner → alerta de entrada

Uso:
    python pipeline_fullbets.py                   # todos os módulos
    python pipeline_fullbets.py --modulo A        # só coleta
    python pipeline_fullbets.py --modulo B        # só treino
    python pipeline_fullbets.py --modulo C        # só scanner
    python pipeline_fullbets.py --modulo D        # só monitor live
    python pipeline_fullbets.py --modulo AC       # coleta + scanner
    python pipeline_fullbets.py --forcar-treino   # força re-treino mesmo sem dados novos

Dependências:
    pip install requests openpyxl pandas scikit-learn joblib
"""

import argparse
import json
import os
import sys
import time
import unicodedata
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import requests
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════
# ▶▶▶  CONFIGURAÇÕES — EDITE AQUI  ◀◀◀
# ═══════════════════════════════════════════════════════════════════

from config import TOKEN  # token em config.py (não versionar)

# ── Arquivos ──────────────────────────────────────────────────────
ARQUIVO_BASE    = "over05ht_sherlock.xlsx"   # base histórica (existente)
ARQUIVO_MODELO  = "model.pkl"                # modelo GBM serializado
ARQUIVO_FEATS   = "feature_list.json"        # lista de features do modelo
ARQUIVO_LOG     = "model_log.csv"            # histórico de AUC/ROI por treino
ARQUIVO_SINAIS  = "sinais_hoje.xlsx"         # output do scanner (módulo C)

# Thresholds ML ─────────────────────────────────────────────────────
THRESHOLD_WATCH   = 0.50   # prob_SH ≥ este valor → incluir na planilha

# Thresholds operacionais por janela (confirmados pelo walk-forward)
# Target: Gol_ate_min35 — entrada no minuto N, saída no min35 com red
THRESHOLD_OPERACIONAL = {
    5 : 0.62,   # ROI +8.3%  | 10/13 semanas +
    10: 0.55,   # ROI +14.0% | 13/13 semanas + ← janela principal
    15: 0.52,   # ROI +22.9% | 13/13 semanas +
}

# ── Critérios mínimos de entrada (decisão manual) ─────────────────
# Estes critérios geram a coluna APTO na planilha sinais_hoje.xlsx
# Você decide a entrada final — o sistema apenas sinaliza o que atende.
CRITERIOS_APTO = {
    "m10_min"      : 0.55,   # prob_m10 ≥ threshold walk-forward
    "m15_min"      : 0.52,   # prob_m15 ≥ threshold walk-forward
    "lg_c_min"     : 100,    # LG_C ≥ 100 (liga com histórico suficiente)
    "odd_emp_min"  : 3.5,    # Odd_Emp ≥ 3.5 (não excessivamente favorito)
}

# ── Controle de retreino ──────────────────────────────────────────
MIN_LINHAS_PARA_TREINO = 30   # retreina se base cresceu ≥ N linhas desde o último treino
AUC_MINIMO             = 0.62  # alerta se AUC cair abaixo disto

# ── APIs ──────────────────────────────────────────────────────────
BASE_LUCY     = "https://gamesapi.fulltraderapps.com/legacy/lucy"
BASE_SHERLOCK = "https://apiprelive.fulltraderapps.com"
INTERVALO_AO_VIVO = 5 * 60   # segundos entre snapshots Lucy no módulo D

# ═══════════════════════════════════════════════════════════════════
# LIGAS ALVO (mesmo dict do script de coleta)
# ═══════════════════════════════════════════════════════════════════

TIMES_LIGAS = {
    "Premier League": [
        "arsenal", "aston villa", "bournemouth", "brentford", "brighton",
        "chelsea", "crystal palace", "everton", "fulham fc", "ipswich",
        "leicester city", "liverpool fc", "manchester city", "manchester united",
        "newcastle united", "nottingham forest", "southampton",
        "tottenham hotspur", "west ham", "wolverhampton",
    ],
    "Championship": [
        "blackburn rovers", "bristol city", "burnley", "cardiff city",
        "coventry city", "derby county", "hull city", "leeds united",
        "luton town", "middlesbrough", "millwall", "norwich city",
        "plymouth argyle", "portsmouth", "preston north end", "qpr",
        "queens park rangers", "sheffield united", "sheffield wednesday",
        "stoke city", "sunderland", "swansea city", "watford", "west brom",
        "west bromwich",
    ],
    "League One": [
        "barnsley", "birmingham city", "bolton wanderers", "bristol rovers",
        "burton albion", "cambridge united", "charlton athletic", "exeter city",
        "huddersfield", "leyton orient", "lincoln city", "northampton",
        "oxford united", "peterborough", "reading fc", "rotherham",
        "shrewsbury", "stevenage", "stockport county", "wigan athletic", "wrexham",
    ],
    "La Liga": [
        "alaves", "atletico madrid", "fc barcelona", "real madrid",
        "sevilla fc", "valencia cf", "villarreal cf", "athletic bilbao",
        "athletic club", "real sociedad", "real betis", "ca osasuna",
        "rc celta", "getafe cf", "girona fc", "las palmas", "cd leganes",
        "rcd mallorca", "rayo vallecano", "rcd espanyol", "deportivo alaves",
        "real valladolid",
    ],
    "Segunda Division": [
        "albacete", "alcorcon", "almeria", "burgos cf", "castellon",
        "eldense", "elche cf", "ferrol", "huesca", "mirandes",
        "cd mirandes", "oviedo", "real oviedo", "racing santander",
        "sd eibar", "sd huesca", "sd ponferradina", "sporting gijon",
        "tenerife", "cd tenerife", "villarreal b", "zaragoza",
        "racing de ferrol", "deportivo coruna",
    ],
    "Bundesliga": [
        "bayer leverkusen", "fc bayern", "borussia dortmund", "rb leipzig",
        "eintracht frankfurt", "sc freiburg", "tsg hoffenheim", "fsv mainz",
        "sv werder", "vfl wolfsburg", "fc augsburg", "vfl bochum",
        "1. fc heidenheim", "holstein kiel", "borussia monchengladbach",
        "fc st. pauli", "vfb stuttgart", "1. fc union berlin",
    ],
    "2. Bundesliga": [
        "1. fc kaiserslautern", "1. fc magdeburg", "1. fc nurnberg",
        "1. fc schalke", "darmstadt 98", "dynamo dresden", "hannover 96",
        "hamburger sv", "hertha berlin", "karlsruher sc", "sc paderborn",
        "ssv ulm", "sv elversberg", "fortuna dusseldorf", "greuther furth",
    ],
    "Serie A": [
        "ac milan", "atalanta", "bologna fc", "cagliari calcio",
        "como 1907", "empoli fc", "acf fiorentina", "genoa cfc",
        "hellas verona", "inter milan", "fc internazionale", "juventus",
        "lazio rome", "us lecce", "ac monza", "ssc napoli",
        "parma calcio", "as roma", "torino fc", "udinese calcio", "venezia fc",
    ],
    "Serie B": [
        "ac cesena", "ascoli calcio", "bari", "benevento calcio",
        "brescia calcio", "carrarese calcio", "catanzaro",
        "cosenza calcio", "cremonese", "frosinone calcio",
        "juve stabia", "mantova", "modena fc", "palermo fc",
        "pisa sporting club", "reggiana", "salernitana",
        "sampdoria", "sassuolo", "spezia calcio", "sudtirol",
    ],
    "Ligue 1": [
        "angers sco", "auxerre", "stade brestois", "le havre ac",
        "rc lens", "losc lille", "olympique lyonnais", "olympique marseille",
        "as monaco", "montpellier hsc", "fc nantes", "ogc nice",
        "paris saint-germain", "paris fc", "stade de reims",
        "stade rennais", "saint-etienne", "rc strasbourg", "toulouse fc",
    ],
    "Ligue 2": [
        "amiens sc", "annecy fc", "bordeaux", "caen", "fc caen",
        "clermont foot", "concarneau", "dunkerque", "grenoble foot",
        "laval", "metz", "fc metz", "niort", "pau fc",
        "quevilly rouen", "red star", "rodez", "sc bastia", "valenciennes",
    ],
    "Eredivisie": [
        "ajax", "az alkmaar", "feyenoord", "sc heerenveen",
        "heracles almelo", "nec nijmegen", "psv eindhoven",
        "sparta rotterdam", "fc twente", "fc utrecht",
        "rkc waalwijk", "willem ii", "go ahead eagles",
        "sc cambuur", "excelsior rotterdam", "fc groningen", "fortuna sittard",
    ],
    "Eerste Divisie": [
        "ado den haag", "almere city", "bv veendam", "dordrecht",
        "fc den bosch", "fc eindhoven", "fc emmen", "fc oss", "fc volendam",
        "helmond sport", "roda jc", "sc telstar", "jong ajax",
        "jong az", "jong psv", "jong utrecht",
    ],
    "Liga NOS": [
        "sl benfica", "sporting cp", "fc porto", "sc braga",
        "vitoria sc", "vitoria guimaraes", "boavista fc",
        "famalicao", "fc famalicao", "estoril praia", "fc arouca",
        "fc vizela", "gil vicente", "moreirense fc", "rio ave fc",
        "santa clara", "cd nacional", "cs maritimo",
    ],
    "Pro League": [
        "rsc anderlecht", "club brugge", "kaa gent", "standard liege",
        "kv mechelen", "kv kortrijk", "royal antwerp", "krc genk",
        "cercle brugge", "oh leuven", "rwdm brussels", "beerschot va",
        "sint-truiden", "charleroi", "sporting charleroi", "ksk beveren",
    ],
    "Super Lig": [
        "fenerbahce", "galatasaray", "besiktas", "trabzonspor",
        "basaksehir", "istanbul basaksehir", "sivasspor", "konyaspor",
        "kayserispor", "antalyaspor", "kasimpasa", "gaziantep",
        "alanyaspor", "samsunspor", "rizespor", "adana demirspor",
    ],
    "Premiership": [
        "celtic fc", "rangers fc", "heart of midlothian", "hibernian fc",
        "aberdeen fc", "dundee fc", "dundee united", "kilmarnock fc",
        "livingston fc", "motherwell fc", "ross county", "st johnstone",
        "st mirren",
    ],
    "Super League": [
        "aek athens", "olympiacos", "panathinaikos", "paok thessaloniki",
        "aris thessaloniki", "panaitolikos gfs", "pas giannina",
        "atromitos", "asteras tripolis", "levadiakos", "ionikos",
    ],
    "HNL": [
        "gnk dinamo zagreb", "hnk rijeka", "hnk hajduk split",
        "nk osijek", "nk lokomotiva", "nk varazdin",
        "nk istra 1961", "nk sibenik", "nk gorica", "nk slaven belupo",
        "dinamo zagreb", "hajduk split", "rijeka", "osijek",
        "lokomotiva zagreb", "slaven koprivnica",
    ],
    "Superliga": [
        "fc kobenhavn", "brondby if", "agf aarhus", "fc midtjylland",
        "odense bk", "randers fc", "silkeborg if", "fc nordsjaelland",
        "vejle bk", "aab aalborg", "hvidovre if",
    ],
    "Allsvenskan": [
        "malmo ff", "ifk goteborg", "djurgarden", "hammarby",
        "ifk norrkoping", "ik sirius", "kalmar ff", "halmstads bk",
        "helsingborg", "mjallby aif", "varbergs bois", "orebro sk",
        "hacken", "bk hacken",
    ],
    "Superettan": [
        "assyriska ff", "brage", "dalkurd ff", "degerfors if",
        "gais", "gefle if", "goteborg fc", "orgryte is",
        "syrianska fc", "utsiktens bk",
    ],
    "Eliteserien": [
        "rosenborg bk", "bodo glimt", "molde fk", "viking fk", "brann",
        "sk brann", "valerenga", "sarpsborg 08", "aalesunds fk",
        "haugesund", "fk haugesund", "lillestrom sk", "stromsgodset",
        "odd grenland", "stabek", "tromso il",
    ],
    "Brasileirao A": [
        "atletico mineiro", "atletico-mg", "atletico paranaense",
        "atletico-pr", "bahia", "botafogo rj", "red bull bragantino",
        "corinthians", "criciuma ec", "cruzeiro ec", "flamengo",
        "fluminense fc", "fortaleza esporte", "gremio porto alegre",
        "sport club internacional", "juventude", "palmeiras", "santos fc",
        "sao paulo fc", "cr vasco da gama",
        "cuiaba", "ceara sc", "america mineiro", "remo",
        "tombense", "londrina", "vila nova", "vitoria",
    ],
    "Brasileirao B": [
        "america mg", "avai fc", "botafogo sp", "chapecoense",
        "coritiba fc", "crb maceio", "figueirense", "goias esporte",
        "guarani fc", "ituano fc", "londrina ec", "mirassol fc",
        "novorizontino", "operario ferroviario", "ponte preta",
        "sampaio correa", "sport recife", "vitoria",
        "nautico", "sao bernardo", "iguatu", "crb",
    ],
    "Argentina": [
        "ca boca juniors", "ca river plate", "ca racing club",
        "ca independiente", "ca san lorenzo", "estudiantes la plata",
        "ca lanus", "ca huracan", "velez sarsfield",
        "argentinos juniors", "rosario central", "talleres cordoba",
        "belgrano cordoba", "atletico tucuman", "godoy cruz",
        "aldosivi", "independiente", "river plate", "boca juniors",
        "racing club", "talleres", "colon santa fe",
    ],
    "Colombia": [
        "atletico nacional", "independiente medellin", "deportivo cali",
        "millonarios fc", "junior barranquilla", "santa fe bogota",
        "once caldas", "deportes tolima", "deportivo pereira",
        "atletico bucaramanga", "america de cali", "cucuta deportivo",
        "envigado", "la equidad", "rionegro aguilas",
    ],
    "Chile": [
        "colo-colo", "universidad de chile", "universidad catolica",
        "cd antofagasta", "cd cobresal", "deportivo nublense",
        "everton de vina", "fc la serena", "deportes magallanes",
        "o'higgins", "san luis", "union espanola", "union la calera", "huachipato",
    ],
    "A-League": [
        "adelaide united", "brisbane roar", "central coast mariners",
        "macarthur fc", "melbourne city", "melbourne victory",
        "newcastle jets", "perth glory", "sydney fc",
        "wellington phoenix", "western sydney", "western united",
    ],
}

# ═══════════════════════════════════════════════════════════════════
# FEATURES DO MODELO (mesmas do treinamento)
# ═══════════════════════════════════════════════════════════════════

SH_FEATS = [
    "SH_LG_Score_C","SH_LG_Score_V","SH_H_Score_C","SH_H_Score_V",
    "SH_Odd_Casa","SH_Odd_Empate","SH_Odd_Visit",
    "SH_FCG_Casa","SH_FCG_Visit","SH_FOHT_Casa","SH_FOHT_Visit",
    "SH_FOFT_Casa","SH_FOFT_Visit","SH_FDHT_Casa","SH_FDHT_Visit",
    "SH_FDFT_Casa","SH_FDFT_Visit",
    "SH_C5_marc_HT_cnt","SH_C5_sofr_HT_cnt","SH_C5_marc_HT_med","SH_C5_sofr_HT_med",
    "SH_C5_cv_marc_HT","SH_C5_cv_sofr_HT",
    "SH_V5_marc_HT_cnt","SH_V5_sofr_HT_cnt","SH_V5_marc_HT_med","SH_V5_sofr_HT_med",
]

LIVE_5M_FEATS = [
    "Chutes_gol_C_5M","Chutes_gol_F_5M","Chutes_fora_C_5M","Chutes_fora_F_5M",
    "Chutes_area_C_5M","Chutes_area_F_5M","Pressao1_C_5M","Pressao1_F_5M",
    "Pressao2_C_5M","Pressao2_F_5M","Odd_Over05HT_5M","Odd_Emp_5M",
    "Odd_Back_C_5M","Odd_Back_F_5M","Odd_Over15_5M","Odd_Over25_5M",
]

# Campos Lucy ao vivo (mapeamento api_key → col_name) — mesmo do script de coleta
CAMPOS_LUCY = [
    ("Chutes_gol_C",    "ChutesNoGolCasaC"),
    ("Chutes_gol_F",    "ChutesNoGolVisitanteC"),
    ("Chutes_fora_C",   "ChutesForaDoGolCasaC"),
    ("Chutes_fora_F",   "ChutesForaDoGolVisitanteC"),
    ("Chutes_area_C",   "ChutesDentroAreaCasaC"),
    ("Chutes_area_F",   "ChutesDentroAreaVisitanteC"),
    ("Pressao1_C",      "Pressao1Casa"),
    ("Pressao1_F",      "Pressao1Visitante"),
    ("Pressao2_C",      "Pressao2Casa"),
    ("Pressao2_F",      "Pressao2Visitante"),
    ("Odd_Back_C",      "BackMoCasaFT"),
    ("Odd_Lay_C",       "LayMoCasaFT"),
    ("Odd_Back_F",      "BackMoVisitanteFT"),
    ("Odd_Emp",         "BackMoEmpateFT"),
    ("Odd_Over05HT",    "BackOver05HT"),
    ("Odd_Over15",      "BackOver15FT"),
    ("Odd_Over25",      "BackOver25FT"),
    ("Odd_Over35",      "BackOver35FT"),
    ("Odd_Under25",     "BackUnder25FT"),
    ("Odd_BTTS",        "BackBttsSim"),
]

# Índices Sherlock (confirmados — mesmo do script de coleta)
IDX_SCORES = {"LG_Score_C":0,"LG_Score_V":1,"H_Score_C":2,"H_Score_V":3}
IDX_MD = {
    "n_ref":2,"marc_HT_cnt":5,"sofr_HT_cnt":6,
    "marc_HT_med":11,"sofr_HT_med":12,"cv_marc_HT":23,"cv_sofr_HT":27,
}
SHERLOCK_CONFIGS = [("SH_C5",22,0),("SH_V5",23,6)]

# ═══════════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════════

def headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Origin":  "https://app.fulltrader.com",
        "Referer": "https://app.fulltrader.com/",
        "Content-Type": "application/json",
    }

def normalizar(nome: str) -> str:
    nfkd = unicodedata.normalize("NFKD", nome or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

def time_na_liga(nome_time: str):
    """
    Verifica se um time pertence a alguma liga alvo.
    Usa match exato ou por token de palavra para evitar falsos positivos.
    Ex: 'league' sozinho NÃO deve casar com 'League One'.
    """
    nome = normalizar(nome_time)
    if not nome or len(nome) < 4:
        return "", False

    for liga, times in TIMES_LIGAS.items():
        for t in times:
            t_norm = normalizar(t)
            # Match exato
            if t_norm == nome:
                return liga, True
            # Match por token: o nome do time alvo deve ter >= 6 chars
            # e deve ser encontrado como substring relevante (não fragmento)
            if len(t_norm) >= 6:
                # O time alvo contido no nome completo do time
                if t_norm in nome and len(nome) <= len(t_norm) + 15:
                    return liga, True
                # O nome do time contido no time alvo (time com nome mais curto)
                if nome in t_norm and len(nome) >= 6:
                    return liga, True
    return "", False


# Mapa de ligas do Sherlock → nomes usados no sistema
# Usado para identificar a liga real pelo campo competition.league_name do Sherlock
LIGA_SHERLOCK_MAP = {
    # Premier League
    "premier league": "Premier League",
    "english premier league": "Premier League",
    # Championship
    "championship": "Championship",
    "english championship": "Championship",
    # League One
    "league one": "League One",
    "english league one": "League One",
    # La Liga
    "la liga": "La Liga",
    "laliga": "La Liga",
    "primera division": "La Liga",
    # Segunda Division
    "segunda division": "Segunda Division",
    "segunda división": "Segunda Division",
    "laliga 2": "Segunda Division",
    # Bundesliga
    "bundesliga": "Bundesliga",
    "1. bundesliga": "Bundesliga",
    # 2. Bundesliga
    "2. bundesliga": "2. Bundesliga",
    "2. bl": "2. Bundesliga",
    # Serie A (Italia)
    "serie a": "Serie A",
    "italian serie a": "Serie A",
    # Serie B (Italia)
    "serie b": "Serie B",
    "italian serie b": "Serie B",
    # Ligue 1
    "ligue 1": "Ligue 1",
    "french ligue 1": "Ligue 1",
    # Ligue 2
    "ligue 2": "Ligue 2",
    "french ligue 2": "Ligue 2",
    # Eredivisie
    "eredivisie": "Eredivisie",
    "dutch eredivisie": "Eredivisie",
    # Eerste Divisie
    "eerste divisie": "Eerste Divisie",
    "dutch eerste divisie": "Eerste Divisie",
    # Liga NOS / Primeira Liga
    "liga nos": "Liga NOS",
    "primeira liga": "Liga NOS",
    "liga portugal": "Liga NOS",
    "liga bwin": "Liga NOS",
    # Pro League (Bélgica)
    "pro league": "Pro League",
    "belgian pro league": "Pro League",
    "jupiler pro league": "Pro League",
    # Super Lig (Turquia)
    "super lig": "Super Lig",
    "turkish super lig": "Super Lig",
    # Premiership (Escócia)
    "premiership": "Premiership",
    "scottish premiership": "Premiership",
    # Championship (Escócia)
    "scottish championship": "Championship",
    # Brasileirao A
    "serie a brasil": "Brasileirao A",
    "brasileirao serie a": "Brasileirao A",
    "campeonato brasileiro serie a": "Brasileirao A",
    # Brasileirao B
    "serie b brasil": "Brasileirao B",
    "brasileirao serie b": "Brasileirao B",
    "campeonato brasileiro serie b": "Brasileirao B",
    # HNL
    "hnl": "HNL",
    "supersport hnl": "HNL",
    "croatian football league": "HNL",
    # Eliteserien
    "eliteserien": "Eliteserien",
    "norwegian eliteserien": "Eliteserien",
    # Allsvenskan
    "allsvenskan": "Allsvenskan",
    "swedish allsvenskan": "Allsvenskan",
    # Colombia
    "liga betplay dimayor": "Colombia",
    "colombian league": "Colombia",
    "primera a": "Colombia",
    # Argentina
    "liga profesional": "Argentina",
    "primera division argentina": "Argentina",
    "torneo binance": "Argentina",
}

LIGAS_VALIDAS = set(TIMES_LIGAS.keys())


def liga_do_sherlock(nome_liga_sh: str) -> str:
    """
    Converte o nome da liga retornado pelo Sherlock para o nome padronizado do sistema.
    Retorna string vazia se a liga não for alvo.
    """
    if not nome_liga_sh:
        return ""
    chave = nome_liga_sh.lower().strip()
    # Busca direta no mapa
    if chave in LIGA_SHERLOCK_MAP:
        return LIGA_SHERLOCK_MAP[chave]
    # Busca parcial
    for k, v in LIGA_SHERLOCK_MAP.items():
        if k in chave or chave in k:
            return v
    return ""

def safe(arr, idx, default=None):
    try:
        v = arr[idx]
        return v if v is not None else default
    except (IndexError, TypeError):
        return default

def sep(titulo="", char="═", n=60):
    if titulo:
        print(f"\n{char*4} {titulo} {char*(n-len(titulo)-6)}")
    else:
        print(char*n)

# ═══════════════════════════════════════════════════════════════════
# FUNÇÕES SHERLOCK
# ═══════════════════════════════════════════════════════════════════

def sherlock_lista(data_str: str) -> list:
    """
    POST /games/list/{data} — confirmado via browser (mudou de GET para POST).

    Response: array de arrays posicionais (18 campos por item):
      [0]  sherlock_id (int)
      [1]  data "YYYY-MM-DD"
      [2]  nome_casa
      [3]  nome_visitante
      [4]  placar_casa_ft
      [5]  placar_visit_ft
      [6]  placar_casa_ht
      [7]  placar_visit_ht
      [8]  id_interno (str)
      [9]  sportradar_id  "sr:match:XXXXX"
      [10] logo_casa
      [11] logo_visit
      [12] timestamp_unix
      [13] país
      [14] liga
      [15] logo_liga
      [16] odd_casa
      [17] total_gols (ou outro campo)

    Retorna lista de dicts normalizados com as chaves esperadas pelo pipeline.
    """
    url = f"{BASE_SHERLOCK}/games/list/{data_str}"
    try:
        r = requests.post(url, headers=headers(), json={}, timeout=20)
        if r.status_code == 401:
            raise RuntimeError("Token expirado!")
        if r.status_code == 403:
            raise RuntimeError("Token inválido ou sem permissão (403).")
        r.raise_for_status()
        raw = r.json()

        if not isinstance(raw, list):
            print(f"  [Sherlock/lista] Resposta inesperada: {type(raw).__name__}")
            return []

        jogos = []
        for item in raw:
            # Novo formato: cada item é um array posicional
            if isinstance(item, list) and len(item) >= 10:
                jogos.append({
                    "id"          : item[0],           # sherlock_id
                    "sportradarId": str(item[9] or ""),# "sr:match:XXXXX"
                    "home"        : {"name": str(item[2] or "")},
                    "away"        : {"name": str(item[3] or "")},
                    "competition" : {"league_name": str(item[14] or "") if len(item) > 14 else ""},
                    "startTime"   : item[12] if len(item) > 12 else None,
                    # odd_casa vem do /list; odd_emp e odd_visit vêm do /games/{id} bloco[24]
                    "odds"        : {
                        "match_odds_home_ft": item[16] if len(item) > 16 else None,
                    },
                })
            # Formato antigo (dict) — compatibilidade retroativa
            elif isinstance(item, dict):
                jogos.append(item)

        return jogos
    except RuntimeError:
        raise
    except Exception as e:
        print(f"  [Sherlock/lista] Erro ({data_str}): {e}")
        return []


def sherlock_detalhe(match_id: int) -> list | None:
    """GET /games/{match_id} — payload completo com todos os blocos."""
    url = f"{BASE_SHERLOCK}/games/{match_id}"
    try:
        r = requests.get(url, headers=headers(), timeout=20)
        if r.status_code in (401, 403, 404):
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [Sherlock/detalhe] Erro ({match_id}): {e}")
        return None


def sherlock_extrair(dados: list) -> dict:
    """
    Extrai todos os campos pré-live do payload /games/{id}.

    Estrutura confirmada via browser (array de 47 blocos):
      bloco[22] → Média e Dispersões casa (configs 5j)
      bloco[23] → Média e Dispersões visit (configs 5j)
      bloco[24] → [odd_casa, odd_emp, odd_visit, ...]   ← CONFIRMADO
      bloco[25] → [LG_C, LG_V, H_C, H_V]               ← confirmado original
      bloco[43] → forças flat (58 posições)             ← confirmado original
      bloco[44] → sportradarId "sr:match:XXXXX"         ← CONFIRMADO
    """
    res = {}

    # Bloco [25] — LG-Score e H-Score (confirmado)
    try:
        b25 = dados[25]
        for campo, idx in IDX_SCORES.items():
            res[f"SH_{campo}"] = safe(b25, idx)
    except (IndexError, TypeError):
        for campo in IDX_SCORES:
            res[f"SH_{campo}"] = None

    # Bloco [24] — Odds 1x2 completas [odd_casa, odd_emp, odd_visit, ...]
    try:
        b24 = dados[24]
        if isinstance(b24, list) and len(b24) >= 3:
            res["SH_Odd_Casa"]   = b24[0]
            res["SH_Odd_Empate"] = b24[1]
            res["SH_Odd_Visit"]  = b24[2]
        else:
            res["SH_Odd_Casa"]   = None
            res["SH_Odd_Empate"] = None
            res["SH_Odd_Visit"]  = None
    except (IndexError, TypeError):
        res["SH_Odd_Casa"]   = None
        res["SH_Odd_Empate"] = None
        res["SH_Odd_Visit"]  = None

    # Bloco [44] — sportradarId (confirmado)
    try:
        res["_sr_id_detalhe"] = dados[44]
    except (IndexError, TypeError):
        res["_sr_id_detalhe"] = None

    # Bloco [43] — Forças (flat array 58 posições) — confirmado original
    try:
        b43 = dados[43]
        if isinstance(b43, list) and len(b43) >= 58:
            res["SH_FCG_Casa"]   = safe(b43, 49)
            res["SH_FCG_Visit"]  = safe(b43, 50)
            res["SH_FOHT_Casa"]  = safe(b43, 52)
            res["SH_FOHT_Visit"] = safe(b43, 53)
            res["SH_FOFT_Casa"]  = safe(b43, 55)
            res["SH_FOFT_Visit"] = safe(b43, 56)
            res["SH_FDHT_Casa"]  = safe(b43, 22)
            res["SH_FDHT_Visit"] = safe(b43, 32)
            res["SH_FDFT_Casa"]  = safe(b43, 20)
            res["SH_FDFT_Visit"] = safe(b43, 30)
        else:
            for k in ["FCG_Casa","FCG_Visit","FOHT_Casa","FOHT_Visit",
                      "FOFT_Casa","FOFT_Visit","FDHT_Casa","FDHT_Visit","FDFT_Casa","FDFT_Visit"]:
                res[f"SH_{k}"] = None
    except (IndexError, TypeError):
        for k in ["FCG_Casa","FCG_Visit","FOHT_Casa","FOHT_Visit",
                  "FOFT_Casa","FOFT_Visit","FDHT_Casa","FDHT_Visit","FDFT_Casa","FDFT_Visit"]:
            res[f"SH_{k}"] = None

    # Blocos [22][23] — Média e Dispersões HT (configs 5j) — confirmado original
    for prefix, bloco_idx, cfg_idx in SHERLOCK_CONFIGS:
        try:
            cfg = dados[bloco_idx][cfg_idx]
        except (IndexError, TypeError):
            for k in IDX_MD:
                res[f"{prefix}_{k}"] = None
            continue
        for campo, idx in IDX_MD.items():
            res[f"{prefix}_{campo}"] = safe(cfg, idx)

    return res


def sherlock_odds_basicas(item: dict) -> dict:
    """
    Extrai campos básicos do item do /games/list.
    O item já foi normalizado por sherlock_lista() para dict com chaves nomeadas.
    Odds completas (empate, visit) vêm do /games/{id} bloco[24].
    """
    odds  = item.get("odds", {}) if isinstance(item.get("odds"), dict) else {}
    comp  = item.get("competition", {}) if isinstance(item.get("competition"), dict) else {}
    home  = item.get("home", {}) if isinstance(item.get("home"), dict) else {}
    away  = item.get("away", {}) if isinstance(item.get("away"), dict) else {}
    return {
        "SH_Liga"      : comp.get("league_name", ""),
        "SH_Odd_Casa"  : odds.get("match_odds_home_ft"),  # do /list
        "SH_Odd_Empate": None,   # será preenchido pelo /games/{id} bloco[24]
        "SH_Odd_Visit" : None,   # será preenchido pelo /games/{id} bloco[24]
        "sherlock_id"  : item.get("id"),
        "sportradar_id": item.get("sportradarId", ""),
        "data_hora"    : item.get("startTime") or item.get("date", ""),
    }


def sherlock_scores_vivo() -> dict:
    """GET /scores — jogos em andamento no 1T."""
    try:
        r = requests.get(f"{BASE_SHERLOCK}/scores", headers=headers(), timeout=15)
        r.raise_for_status()
        return {item["id"]: item for item in r.json() if "id" in item}
    except Exception as e:
        print(f"  [Sherlock/scores] Erro: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════
# FUNÇÕES LUCY
# ═══════════════════════════════════════════════════════════════════

def lucy_pagina(data_str: str, pagina: int) -> dict:
    params = {"page": pagina, "order": "ASC",
              "startDate": data_str, "endDate": data_str, "perPage": 100}
    try:
        r = requests.get(BASE_LUCY, headers=headers(), params=params, timeout=20)
        if r.status_code == 401:
            raise RuntimeError("Token expirado!")
        r.raise_for_status()
        return r.json()
    except RuntimeError:
        raise
    except Exception as e:
        print(f"  [Lucy/pagina] Erro p{pagina} ({data_str}): {e}")
        return {}


def lucy_buscar_jogos(data_str: str, incluir_dia_anterior: bool = False) -> list:
    """
    Busca todos os jogos das ligas alvo em uma data (Lucy).
    
    incluir_dia_anterior=True: usado pelo módulo C (scanner pré-live)
        para cobrir jogos que iniciaram perto da meia-noite UTC.
    incluir_dia_anterior=False (padrão): usado pelo módulo A (coleta histórica)
        onde a data alvo é exata e não deve buscar dias anteriores.
    """
    encontrados = []
    ids_vistos = set()

    # Deltas: módulo A usa só [0], módulo C pode usar [-1, 0]
    deltas = [-1, 0] if incluir_dia_anterior else [0]

    for delta in deltas:
        dt = (datetime.strptime(data_str, "%Y-%m-%d") + timedelta(days=delta)).strftime("%Y-%m-%d")
        pagina = 1
        while True:
            data = lucy_pagina(dt, pagina)
            if not data:
                break
            jogos = data.get("result", [])
            if not jogos:
                break
            for j in jogos:
                mid = j.get("sport_event_id", "")
                if mid and mid in ids_vistos:
                    continue  # dedup por Match_ID
                liga_c, ok_c = time_na_liga(j.get("NomeCasa", ""))
                liga_f, ok_f = time_na_liga(j.get("NomeVisitante", ""))
                if ok_c or ok_f:
                    liga_inferida = liga_c if ok_c else liga_f
                    if liga_inferida in LIGAS_VALIDAS:
                        j["_liga"] = liga_inferida
                        j["_data"] = dt
                        encontrados.append(j)
                        if mid:
                            ids_vistos.add(mid)
            total_pag = data.get("numberPages", 1)
            if pagina >= total_pag:
                break
            pagina += 1
            time.sleep(0.3)
    return encontrados


def lucy_minuto(match_id: str, minuto: int) -> dict:
    try:
        r = requests.get(
            f"{BASE_LUCY}/{match_id}", headers=headers(),
            params={"minute": minuto, "period": 1}, timeout=15
        )
        if r.status_code == 401:
            raise RuntimeError("Token expirado!")
        r.raise_for_status()
        dados = r.json().get("data")
        return dados if dados else {}   # nunca retorna None
    except RuntimeError:
        raise
    except Exception as e:
        print(f"  [Lucy/minuto] Erro ({match_id}, min{minuto}): {e}")
        return {}


def lucy_extrair_minuto(dados: dict, minuto: int) -> dict:
    return {f"{col}_{minuto}M": dados.get(api_key, 0) for col, api_key in CAMPOS_LUCY}


# ═══════════════════════════════════════════════════════════════════
# MÓDULO A — COLETA HISTÓRICA (ontem → base)
# ═══════════════════════════════════════════════════════════════════

def modulo_A():
    sep("MÓDULO A — Coleta histórica (ontem)")

    ontem = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Data alvo: {ontem}")

    # Verificar se ontem já está na base
    if os.path.exists(ARQUIVO_BASE):
        df_ex = pd.read_excel(ARQUIVO_BASE)
        ja_coletado = df_ex["Data"].astype(str).str.startswith(ontem).any()
        if ja_coletado:
            n_ontem = df_ex["Data"].astype(str).str.startswith(ontem).sum()
            print(f"  {ontem} já na base ({n_ontem} linhas) — pulando coleta.")
            return 0
    else:
        df_ex = pd.DataFrame()

    # Sherlock: índice do dia
    print(f"  Construindo índice Sherlock ({ontem})...")
    lista_sh = sherlock_lista(ontem)
    indice_sh = {}
    for item in lista_sh:
        sr_id = item.get("sportradarId", "")
        if sr_id:
            indice_sh[sr_id] = sherlock_odds_basicas(item)
    print(f"  Sherlock: {len(indice_sh)} jogos indexados")

    # Lucy: buscar jogos de ontem (apenas a data exata, sem dia anterior)
    print(f"  Buscando jogos Lucy ({ontem})...")
    jogos = lucy_buscar_jogos(ontem, incluir_dia_anterior=False)
    print(f"  Lucy: {len(jogos)} jogos nas ligas alvo")

    if not jogos:
        print("  Nenhum jogo encontrado — sem coleta.")
        return 0

    # Coletar cada jogo (modo histórico = sem espera)
    novas = 0
    # Set de Match_IDs já existentes na base para evitar duplicatas
    ids_existentes = set(df_ex["Match_ID"].astype(str).tolist()) if "Match_ID" in df_ex.columns and len(df_ex) > 0 else set()

    for jogo in jogos:
        match_id  = jogo["sport_event_id"]
        nome_casa = jogo.get("NomeCasa", "?").title()
        nome_fora = jogo.get("NomeVisitante", "?").title()
        liga      = jogo.get("_liga", "?")

        # Pular se Match_ID já existe na base
        if str(match_id) in ids_existentes:
            print(f"  ↷  [{liga}] {nome_casa} vs {nome_fora} — já na base, pulando")
            continue

        # Linha base
        linha = {
            "Jogo"            : f"{nome_casa} vs {nome_fora}",
            "Liga"            : liga,
            "Data"            : jogo.get("_data", ontem),
            "Match_ID"        : match_id,
            "Placar_FT"       : jogo.get("Placar", ""),
            "Placar_HT"       : None,
            "Over05_HT"       : 0,
            "Gol_min_HT"      : None,
            "Filtro_0x0_min5" : 0,
            "Min_Coleta_Final": None,
            "SH_match"        : False,
        }

        # Injetar Sherlock
        entry = indice_sh.get(match_id)
        if entry:
            linha.update(entry)
            linha["SH_match"] = True
            sh_id = entry.get("sherlock_id")
            if sh_id:
                dados_sh = sherlock_detalhe(sh_id)
                if dados_sh:
                    linha.update(sherlock_extrair(dados_sh))

        # Inicializar colunas live com zeros
        for m in [5, 10, 15, 20, 25, 30, 35]:
            for col, _ in CAMPOS_LUCY:
                linha[f"{col}_{m}M"] = 0

        # Coleta Lucy minuto a minuto (histórico: sem espera)
        gols_antes = None
        for idx, minuto in enumerate([5, 10, 15, 20, 25, 30, 35]):
            try:
                dados = lucy_minuto(match_id, minuto)
            except RuntimeError:
                raise
            except Exception:
                time.sleep(1)
                continue

            if not dados:
                continue

            gc    = int(dados.get("GolsCasa", 0) or 0)
            gf    = int(dados.get("GolsVisitante", 0) or 0)
            total = gc + gf

            # ── Min5: filtro 0x0 ──────────────────────────────────────────────
            if idx == 0:
                linha.update(lucy_extrair_minuto(dados, minuto))
                if total == 0:
                    linha["Filtro_0x0_min5"]  = 1
                    linha["Placar_HT"]        = f"{gc}x{gf}"
                    linha["Min_Coleta_Final"] = minuto
                    gols_antes = 0
                else:
                    linha["Placar_HT"] = f"{gc}x{gf}"
                    break  # gol antes do min5 → Filtro=0

            # ── Min10-35: verificar gol ANTES de salvar (anti-leakage) ────────
            else:
                if gols_antes is not None and total > gols_antes and linha["Over05_HT"] == 0:
                    minuto_limpo              = [5,10,15,20,25,30,35][idx - 1]
                    linha["Over05_HT"]        = 1
                    linha["Gol_min_HT"]       = minuto
                    linha["Placar_HT"]        = f"{gc}x{gf}"
                    linha["Min_Coleta_Final"] = minuto_limpo
                    break  # dados do minuto do gol NÃO são salvos

                linha.update(lucy_extrair_minuto(dados, minuto))
                linha["Placar_HT"]        = f"{gc}x{gf}"
                linha["Min_Coleta_Final"] = minuto
                gols_antes = total

            time.sleep(0.3)

        # Append na base
        df_novo  = pd.DataFrame([linha])
        df_ex    = pd.concat([df_ex, df_novo], ignore_index=True)
        ids_existentes.add(str(match_id))
        novas   += 1
        print(f"  ✓  [{liga}] {nome_casa} vs {nome_fora}  "
              f"Filtro={linha['Filtro_0x0_min5']}  Over={linha['Over05_HT']}")

    # Salvar base atualizada
    df_ex.to_excel(ARQUIVO_BASE, index=False)
    print(f"\n  Base atualizada: {len(df_ex)} linhas totais (+{novas} novas)")
    return novas


# ═══════════════════════════════════════════════════════════════════
# MÓDULO B — TREINO DOS MODELOS (delega ao model_trainer.py)
# ═══════════════════════════════════════════════════════════════════

def modulo_B(forcar=False):
    sep("MÓDULO B — Treino dos modelos (3 janelas)")

    if not os.path.exists(ARQUIVO_BASE):
        print("  Base não encontrada — pulando treino.")
        return

    # Importar o trainer dedicado
    try:
        import importlib.util, sys as _sys
        spec = importlib.util.spec_from_file_location(
            "model_trainer",
            os.path.join(os.path.dirname(__file__), "model_trainer.py")
        )
        trainer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(trainer)
    except Exception as e:
        print(f"  ⚠ model_trainer.py não encontrado: {e}")
        print("  Coloque model_trainer.py na mesma pasta que pipeline_fullbets.py")
        return

    df = pd.read_excel(ARQUIVO_BASE)
    print(f"  Base: {len(df):,} linhas | válidos: {(df['Filtro_0x0_min5']==1).sum():,}")

    # Treinar os 3 modelos: min5, min10, min15
    # Target correto: gol entre min_entrada e min35 (janela de trading)
    for minuto in [5, 10, 15]:
        resultado = trainer.treinar_modelo(minuto, df, salvar=True, forcar=forcar)
        if resultado and not resultado.get("adiado"):
            auc = resultado.get("auc", 0)
            roi = resultado.get("roi_op", 0)
            if auc < AUC_MINIMO:
                print(f"  ⚠ min{minuto}: AUC {auc:.4f} < {AUC_MINIMO} — verificar base!")


# ═══════════════════════════════════════════════════════════════════
# MÓDULO C — SCANNER PRÉ-LIVE (jogos do dia)
# ═══════════════════════════════════════════════════════════════════

def _carregar_modelo(minuto: int = 10):
    """
    Carrega modelo e features para a janela especificada.
    Prioridade: model_m{minuto}.pkl (novo trainer) → model.pkl (legado)
    """
    arq_novo   = f"model_m{minuto}.pkl"
    arq_feats  = f"features_m{minuto}.json"
    arq_legado = ARQUIVO_MODELO

    # Novo formato (model_trainer.py)
    if os.path.exists(arq_novo) and os.path.exists(arq_feats):
        modelo = joblib.load(arq_novo)
        with open(arq_feats) as f:
            feats = json.load(f)
        return modelo, feats

    # Fallback legado (model.pkl + feature_list.json)
    if os.path.exists(arq_legado) and os.path.exists(ARQUIVO_FEATS):
        print(f"  ⚠ Usando modelo legado ({arq_legado}) — rode model_trainer.py para atualizar")
        modelo = joblib.load(arq_legado)
        with open(ARQUIVO_FEATS) as f:
            feats = json.load(f)
        return modelo, feats

    raise FileNotFoundError(
        f"Modelo min{minuto} não encontrado. "
        f"Rode: python model_trainer.py --minuto {minuto}"
    )


def _prob_SH(campos_sh: dict, modelo, feats: list) -> float:
    """
    Probabilidade pré-live (apenas SH_*).
    Features live ausentes preenchidas com 0.
    """
    row = {f: float(campos_sh.get(f) or 0) for f in feats}
    X   = pd.DataFrame([row])
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)
    return float(modelo.predict_proba(X)[0, 1])


def modulo_C():
    sep("MÓDULO C — Scanner pré-live")

    hoje   = date.today().strftime("%Y-%m-%d")
    amanha = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Buscando jogos: hoje ({hoje}) e amanhã ({amanha})")

    # Carregar modelos das 3 janelas
    modelos = {}
    for m in [5, 10, 15]:
        try:
            modelos[m] = _carregar_modelo(m)
            print(f"  Modelo min{m}: {len(modelos[m][1])} features")
        except FileNotFoundError as e:
            print(f"  ⚠ min{m}: {e}")

    if not modelos:
        print("  Nenhum modelo disponível — rode: python model_trainer.py")
        return

    # Modelo principal = min10 (melhor equilíbrio volume/ROI/WF)
    # Fallback para min5 se min10 não disponível
    mod_principal = 10 if 10 in modelos else (5 if 5 in modelos else 15)
    th_principal  = THRESHOLD_WATCH  # prob_SH mínima para entrar na watchlist

    sinais = []

    for data_alvo in [hoje, amanha]:
        lista = sherlock_lista(data_alvo)
        if not lista:
            print(f"  Sherlock ({data_alvo}): nenhum jogo")
            continue
        print(f"  Sherlock ({data_alvo}): {len(lista)} jogos totais")

        jogos_liga = []
        for item in lista:
            home_name = item.get("home", {}).get("name", "")
            away_name = item.get("away", {}).get("name", "")
            sh_liga   = item.get("competition", {}).get("league_name", "") if isinstance(item.get("competition"), dict) else ""

            # 1. Tentar mapear pela liga real do Sherlock (mais confiável)
            liga_real = liga_do_sherlock(sh_liga)

            # 2. Fallback: inferir pelo nome do time (menos confiável)
            if not liga_real:
                home_norm = normalizar(home_name)
                away_norm = normalizar(away_name)
                liga_h, ok_h = time_na_liga(home_norm)
                liga_a, ok_a = time_na_liga(away_norm)
                if ok_h or ok_a:
                    liga_real = liga_h if ok_h else liga_a

            # Só inclui se a liga for reconhecida como alvo
            if liga_real and liga_real in LIGAS_VALIDAS:
                item["_liga"] = liga_real
                jogos_liga.append(item)

        print(f"  Ligas alvo: {len(jogos_liga)} jogos")

        for item in jogos_liga:
            sh_id   = item.get("id")
            sr_id   = item.get("sportradarId", "")
            home    = item.get("home", {}).get("name", "?")
            away    = item.get("away", {}).get("name", "?")
            liga    = item.get("_liga", "?")
            horario = item.get("startTime") or item.get("date", "")

            campos = sherlock_odds_basicas(item)
            if sh_id:
                dados_det = sherlock_detalhe(sh_id)
                if dados_det:
                    campos.update(sherlock_extrair(dados_det))

            # Calcular prob para cada janela disponível
            probs = {}
            for m, (mod, feats) in modelos.items():
                probs[m] = round(_prob_SH(campos, mod, feats), 4)

            prob_principal = probs.get(mod_principal, 0.0)
            th_watch_op    = THRESHOLD_OPERACIONAL.get(mod_principal, THRESHOLD_WATCH)
            status = "WATCH" if prob_principal >= th_principal else "SKIP"

            # ── Critérios mínimos de entrada (coluna APTO) ──────────
            m10_val   = probs.get(10, 0) or 0
            m15_val   = probs.get(15, 0) or 0
            lg_c_val  = campos.get("SH_LG_Score_C") or 0
            odd_emp   = campos.get("SH_Odd_Empate") or 0

            falhas = []
            if m10_val  < CRITERIOS_APTO["m10_min"]:
                falhas.append(f"m10={m10_val:.2f}<{CRITERIOS_APTO['m10_min']}")
            if m15_val  < CRITERIOS_APTO["m15_min"]:
                falhas.append(f"m15={m15_val:.2f}<{CRITERIOS_APTO['m15_min']}")
            if lg_c_val < CRITERIOS_APTO["lg_c_min"]:
                falhas.append(f"LG_C={lg_c_val}<{CRITERIOS_APTO['lg_c_min']}")
            if odd_emp  < CRITERIOS_APTO["odd_emp_min"]:
                falhas.append(f"Odd_Emp={odd_emp:.1f}<{CRITERIOS_APTO['odd_emp_min']}")

            apto   = "SIM" if not falhas else "NÃO"
            motivo = "OK" if not falhas else " | ".join(falhas)

            sinais.append({
                "Data"       : data_alvo,
                "Hora"       : horario,
                "Liga"       : liga,
                "Casa"       : home,
                "Visitante"  : away,
                # Probabilidades pré-live das 3 janelas
                "m5"         : probs.get(5),
                "m10"        : probs.get(10),
                "m15"        : probs.get(15),
                # Critérios Sherlock
                "LG_C"       : campos.get("SH_LG_Score_C"),
                "LG_V"       : campos.get("SH_LG_Score_V"),
                "H_Score_C"  : campos.get("SH_H_Score_C"),
                "Odd_Casa"   : campos.get("SH_Odd_Casa"),
                "Odd_Emp"    : campos.get("SH_Odd_Empate"),
                "Odd_Visit"  : campos.get("SH_Odd_Visit"),
                # Decisão
                "APTO"       : apto,
                "MOTIVO"     : motivo,
                # IDs internos (para referência)
                "Match_ID"   : sr_id,
                "Sherlock_ID": sh_id,
            })

            flag = "✔ APTO" if apto == "SIM" else "·     "
            p10  = f"{probs.get(10, 0):.3f}" if 10 in probs else "  —  "
            p15  = f"{probs.get(15, 0):.3f}" if 15 in probs else "  —  "
            print(f"  {flag}  [{liga}] {home} vs {away}  "
                  f"m10={p10}  m15={p15}")

        time.sleep(0.5)

    if not sinais:
        print("  Nenhum sinal gerado.")
        return

    df_sinais = pd.DataFrame(sinais).sort_values("m10", ascending=False)

    # APTO primeiro, depois por m10 desc
    df_sinais = pd.concat([
        df_sinais[df_sinais["APTO"] == "SIM"].sort_values("m10", ascending=False),
        df_sinais[df_sinais["APTO"] == "NÃO"].sort_values("m10", ascending=False),
    ]).reset_index(drop=True)

    df_sinais.to_excel(ARQUIVO_SINAIS, index=False)

    n_apto = (df_sinais["APTO"] == "SIM").sum()
    print(f"\n  {len(sinais)} jogos analisados | {n_apto} APTOS para entrada")
    print(f"  Sinais salvos → {ARQUIVO_SINAIS}")

    print(f"\n  TOP 5 APTOS (modelo min10):")
    print(f"  {'Liga':<20} {'Casa':<20} {'Vis':<20} {'m5':>6} {'m10':>6} {'m15':>6}")
    print("  " + "─"*76)
    top5 = df_sinais[df_sinais["APTO"] == "SIM"].head(5)
    for _, r in top5.iterrows():
        m5  = f"{r['m5']:.3f}"  if r['m5']  is not None else "  —  "
        m10 = f"{r['m10']:.3f}" if r['m10'] is not None else "  —  "
        m15 = f"{r['m15']:.3f}" if r['m15'] is not None else "  —  "
        print(f"  {r['Liga']:<20} {r['Casa']:<20} {r['Visitante']:<20} "
              f"{m5:>6} {m10:>6} {m15:>6}")


# ═══════════════════════════════════════════════════════════════════
# MÓDULO D — MONITOR LIVE MIN5 (alertas de entrada)
# ═══════════════════════════════════════════════════════════════════

def _prob_final(campos_sh: dict, dados_live: dict, modelo, feats: list,
               minuto: int) -> float:
    """
    Probabilidade final com SH_* + features live do minuto de entrada.
    """
    row = {f: float(campos_sh.get(f) or 0) for f in feats}
    # Sobrescrever com valores live reais
    for col, api_key in CAMPOS_LUCY:
        feat = f"{col}_{minuto}M"
        if feat in feats:
            row[feat] = float(dados_live.get(api_key) or 0)
    X = pd.DataFrame([row])
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)
    return float(modelo.predict_proba(X)[0, 1])


def _emitir_alerta(jogo: dict, prob_final: float, dados_live: dict,
                   odd_live: float, minuto: int, janela: int):
    """Imprime alerta de entrada formatado."""
    th = THRESHOLD_OPERACIONAL.get(janela, THRESHOLD_ENTRADA)
    print()
    print("  " + "★"*55)
    print(f"  ENTRADA — {jogo['Casa']} vs {jogo['Visitante']}")
    print(f"  Liga      : {jogo['Liga']}")
    print(f"  Janela    : entrada no min{minuto} | modelo min{janela}")
    print(f"  Prob final: {prob_final:.3f}  ({prob_final*100:.1f}%)  th≥{th}")
    print(f"  Odd live  : {odd_live:.2f}  → Back Over 0.5 HT agora")
    print(f"  Saída     : min35 com red se não sair gol")
    print(f"  SH        : LG_C={jogo.get('LG_C','?')}  "
          f"H_Score={jogo.get('H_Score_C','?')}  "
          f"Odd_Emp={jogo.get('Odd_Emp','?')}")
    p1c  = dados_live.get("Pressao1Casa", 0)
    p2c  = dados_live.get("Pressao2Casa", 0)
    area = dados_live.get("ChutesDentroAreaCasaC", 0)
    print(f"  Live min{minuto} : P1C={p1c}  P2C={p2c}  Chutes_area={area}")
    print("  " + "★"*55)
    print()


def modulo_D():
    sep("MÓDULO D — Monitor live (2 etapas)")

    if not os.path.exists(ARQUIVO_SINAIS):
        print(f"  {ARQUIVO_SINAIS} não encontrado — rode o Módulo C primeiro.")
        return

    # Carregar modelos das janelas disponíveis
    modelos = {}
    for m in [5, 10, 15]:
        try:
            modelos[m] = _carregar_modelo(m)
        except FileNotFoundError:
            pass

    if not modelos:
        print("  Nenhum modelo disponível — rode: python model_trainer.py")
        return

    # Definir janela principal de operação
    janela_op = 10 if 10 in modelos else (5 if 5 in modelos else 15)
    th_op     = THRESHOLD_OPERACIONAL.get(janela_op, THRESHOLD_ENTRADA)
    print(f"  Janela operacional: min{janela_op} | threshold: ≥{th_op}")

    # Carregar watchlist de hoje
    df = pd.read_excel(ARQUIVO_SINAIS)
    hoje = date.today().strftime("%Y-%m-%d")
    watchlist = df[
        (df["status"] == "WATCH") &
        (df["Data"].astype(str) == hoje)
    ].to_dict("records")

    if not watchlist:
        print(f"  Nenhum jogo em WATCH para hoje ({hoje}).")
        return

    print(f"  Watchlist: {len(watchlist)} jogos")
    for j in watchlist:
        p10 = j.get("prob_m10") or j.get("prob_SH", 0)
        print(f"    [{j['Liga']}] {j['Casa']} vs {j['Visitante']}  "
              f"prob_m10={p10:.3f}")

    print(f"\n  Monitorando... (ctrl+C para parar)\n")
    print(f"  Lógica:")
    print(f"    1. Min5  → confirmar 0x0 (filtro de entrada)")
    print(f"    2. Min{janela_op} → coletar features live + recalcular prob")
    print(f"    3. Se prob_final ≥ {th_op} → ALERTA DE ENTRADA")
    print()

    # Estado por jogo: None → aguardando min5 → aguardando min10 → processado
    estado = {j.get("Match_ID", ""): "aguardando" for j in watchlist}
    # guardará dados do min5 para usar no cálculo final
    dados_min5_cache = {}

    while True:
        scores_vivo = sherlock_scores_vivo()

        for jogo in watchlist:
            sr_id = jogo.get("Match_ID", "")
            if estado.get(sr_id) == "processado":
                continue

            score      = scores_vivo.get(sr_id, {})
            minuto_at  = score.get("minute", 0)
            periodo    = score.get("period", 0)

            if periodo != 1:
                continue

            # ── ETAPA 1: Min5 — confirmar 0x0 ────────────────────────
            if estado[sr_id] == "aguardando" and 5 <= minuto_at <= 7:
                print(f"  Min{minuto_at}: {jogo['Casa']} vs {jogo['Visitante']}")
                try:
                    dados_5m = lucy_minuto(sr_id, 5)
                except Exception as e:
                    print(f"  Erro Lucy min5: {e}")
                    continue

                if not dados_5m:
                    print(f"  Sem dados min5 — aguardando próximo ciclo...")
                    continue

                gc = int(dados_5m.get("GolsCasa", 0) or 0)
                gf = int(dados_5m.get("GolsVisitante", 0) or 0)
                if gc + gf > 0:
                    print(f"  Gol antes min5 ({gc}x{gf}) — descartado")
                    estado[sr_id] = "processado"
                    continue

                print(f"  0x0 confirmado — aguardando min{janela_op}...")
                dados_min5_cache[sr_id] = dados_5m
                estado[sr_id] = "aguardando_min10"

            # ── ETAPA 2: MinX — coletar features e calcular prob ──────
            elif estado[sr_id] == "aguardando_min10" and \
                 janela_op <= minuto_at <= janela_op + 2:

                print(f"  Min{minuto_at}: calculando prob final para "
                      f"{jogo['Casa']} vs {jogo['Visitante']}...")
                try:
                    dados_live = lucy_minuto(sr_id, janela_op)
                except Exception as e:
                    print(f"  Erro Lucy min{janela_op}: {e}")
                    continue

                if not dados_live:
                    print(f"  Sem dados min{janela_op} — aguardando próximo ciclo...")
                    continue

                # Verificar se ainda 0x0
                gc = int(dados_live.get("GolsCasa", 0) or 0)
                gf = int(dados_live.get("GolsVisitante", 0) or 0)
                if gc + gf > 0:
                    print(f"  Gol detectado no min{janela_op} ({gc}x{gf}) — descartado")
                    estado[sr_id] = "processado"
                    continue

                # Recuperar campos SH
                campos_sh = {
                    "SH_LG_Score_C": jogo.get("LG_C"),
                    "SH_LG_Score_V": jogo.get("LG_V"),
                    "SH_H_Score_C" : jogo.get("H_Score_C"),
                    "SH_Odd_Empate": jogo.get("Odd_Emp"),
                    "SH_Odd_Casa"  : jogo.get("Odd_Casa"),
                }
                sh_id = jogo.get("Sherlock_ID")
                if sh_id:
                    dados_det = sherlock_detalhe(sh_id)
                    if dados_det:
                        campos_sh.update(sherlock_extrair(dados_det))

                # Adicionar features do min5 ao contexto (já coletadas na etapa 1)
                if sr_id in dados_min5_cache:
                    for col, api_key in CAMPOS_LUCY:
                        campos_sh[f"{col}_5M"] = dados_min5_cache[sr_id].get(api_key, 0)

                # Calcular probabilidade final com modelo da janela operacional
                mod_op, feats_op = modelos[janela_op]
                prob_f   = _prob_final(campos_sh, dados_live, mod_op, feats_op, janela_op)
                odd_live = float(dados_live.get("BackOver05HT") or 0)

                print(f"  Prob final: {prob_f:.3f}  |  "
                      f"Odd live: {odd_live:.2f}  |  threshold: {th_op}")

                if prob_f >= th_op:
                    _emitir_alerta(jogo, prob_f, dados_live, odd_live, janela_op, janela_op)
                else:
                    print(f"  prob {prob_f:.3f} < {th_op} — não entra")

                estado[sr_id] = "processado"

        # Verificar se todos foram processados
        if all(v == "processado" for v in estado.values()):
            print("  Todos os jogos da watchlist processados.")
            break

        time.sleep(30)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="FULLBETS Pipeline Over 0.5 HT"
    )
    parser.add_argument(
        "--modulo", default="ABC",
        help="Módulos a executar: A=coleta, B=treino, C=scanner. "
             "Pode combinar: AB, AC, BC, etc. Default: ABC"
    )
    parser.add_argument(
        "--forcar-treino", action="store_true",
        help="Força re-treino mesmo sem dados novos suficientes"
    )
    args = parser.parse_args()

    if not TOKEN or TOKEN == "COLE_SEU_TOKEN_JWT_AQUI":
        print("ERRO: Cole seu token JWT em TOKEN (linha ~60).")
        sys.exit(1)

    modulos = args.modulo.upper()
    forcar  = args.forcar_treino

    sep(f"FULLBETS PIPELINE — {date.today()} — módulos: {modulos}", "═", 60)
    inicio = time.time()

    try:
        if "A" in modulos:
            novas = modulo_A()
            if "B" in modulos and novas > 0:
                forcar = True

        if "B" in modulos:
            modulo_B(forcar=forcar)

        if "C" in modulos:
            modulo_C()

    except RuntimeError as e:
        print(f"\n  ERRO CRÍTICO: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n  Interrompido pelo usuário.")

    elapsed = time.time() - inicio
    sep(f"Pipeline concluído em {elapsed:.0f}s", "═", 60)


if __name__ == "__main__":
    main()
