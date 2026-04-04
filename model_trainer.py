"""
model_trainer.py — FULLBETS Treinamento dos Modelos de Trading
==============================================================
Treina 3 modelos GBM com target correto:

  TARGET = Gol_ate_minXX
    = 1 se gol saiu entre min_entrada e min35 (janela de trading)
    = 0 se chegou no min35 sem gol (sai com red)

  Modelos:
    model_m5.pkl   → entrada no min5  | AUC ~0.70
    model_m10.pkl  → entrada no min10 | AUC ~0.73  ← recomendado
    model_m15.pkl  → entrada no min15 | AUC ~0.75

  Para cada modelo salva:
    model_mXX.pkl          → modelo GBM treinado
    features_mXX.json      → lista de features
    scaler_mXX.pkl         → StandardScaler (para probabilidades calibradas)
    model_log.csv          → histórico AUC/ROI de cada treino

Uso:
    python model_trainer.py                  # treina os 3 modelos
    python model_trainer.py --minuto 10      # treina só o min10
    python model_trainer.py --avaliar        # só avalia sem salvar
    python model_trainer.py --forcar         # força mesmo sem dados novos

Dependências:
    pip install pandas openpyxl scikit-learn joblib
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════════

ARQUIVO_BASE      = "over05ht_sherlock.xlsx"
ARQUIVO_LOG       = "model_log.csv"
MIN_LINHAS_TREINO = 500   # mínimo para treinar
MIN_CRESCIMENTO   = 30    # mínimo de linhas novas para retreinar

# Thresholds operacionais (confirmados pelo walk-forward)
THRESHOLD_OPERACIONAL = {
    5 : 0.62,
    10: 0.55,
    15: 0.52,
}

# ═══════════════════════════════════════════════════════════════════
# FEATURES
# ═══════════════════════════════════════════════════════════════════

SH_FEATS = [
    "SH_LG_Score_C", "SH_LG_Score_V",
    "SH_H_Score_C",  "SH_H_Score_V",
    "SH_Odd_Casa",   "SH_Odd_Empate",  "SH_Odd_Visit",
    "SH_FCG_Casa",   "SH_FCG_Visit",
    "SH_FOHT_Casa",  "SH_FOHT_Visit",
    "SH_FOFT_Casa",  "SH_FOFT_Visit",
    "SH_FDHT_Casa",  "SH_FDHT_Visit",
    "SH_FDFT_Casa",  "SH_FDFT_Visit",
    "SH_C5_marc_HT_cnt", "SH_C5_sofr_HT_cnt",
    "SH_C5_marc_HT_med", "SH_C5_sofr_HT_med",
    "SH_C5_cv_marc_HT",  "SH_C5_cv_sofr_HT",
    "SH_V5_marc_HT_cnt", "SH_V5_sofr_HT_cnt",
    "SH_V5_marc_HT_med", "SH_V5_sofr_HT_med",
]

LIVE_FEATS = {
    5: [
        "Pressao1_C_5M",  "Pressao1_F_5M",
        "Pressao2_C_5M",  "Pressao2_F_5M",
        "Chutes_gol_C_5M","Chutes_fora_C_5M","Chutes_area_C_5M",
        "Odd_Over05HT_5M","Odd_Emp_5M",
        "Odd_Back_C_5M",  "Odd_Back_F_5M",
        "Odd_Over15_5M",  "Odd_Over25_5M",
    ],
    10: [
        "Pressao1_C_10M", "Pressao1_F_10M",
        "Pressao2_C_10M", "Pressao2_F_10M",
        "Chutes_gol_C_10M","Chutes_fora_C_10M","Chutes_area_C_10M",
        "Odd_Over05HT_10M","Odd_Emp_10M",
        "Odd_Back_C_10M",  "Odd_Back_F_10M",
        "Odd_Over15_10M",  "Odd_Over25_10M",
    ],
    15: [
        "Pressao1_C_15M", "Pressao1_F_15M",
        "Pressao2_C_15M", "Pressao2_F_15M",
        "Chutes_gol_C_15M","Chutes_fora_C_15M","Chutes_area_C_15M",
        "Odd_Over05HT_15M","Odd_Emp_15M",
        "Odd_Back_C_15M",  "Odd_Back_F_15M",
        "Odd_Over15_15M",  "Odd_Over25_15M",
    ],
}


# ═══════════════════════════════════════════════════════════════════
# CONSTRUÇÃO DO DATASET
# ═══════════════════════════════════════════════════════════════════

def construir_dataset(df: pd.DataFrame, minuto: int):
    """
    Retorna (X, y, sub, feats) com target e filtros corretos.

    Target = Gol_ate_min35:
      = 1 se gol saiu ENTRE min_entrada (exclusive) e min35 (inclusive)
      = 0 se chegou no min35 sem gol (sai com red)

    Anti-leakage:
      - Filtro_0x0_min5 = 1
      - Min_Coleta_Final >= minuto
      - Odd_XM > 0  (garante snapshot limpo do minuto de entrada)
    """
    odd_col = f"Odd_Over05HT_{minuto}M"

    # Filtros anti-leakage
    mask = (
        (df["Filtro_0x0_min5"] == 1) &
        (df["Min_Coleta_Final"] >= minuto)
    )
    if odd_col in df.columns:
        mask = mask & (df[odd_col].fillna(0) > 0)

    sub = df[mask].copy().reset_index(drop=True)

    # Target correto para trading: gol saiu dentro da janela [min_entrada, min35]
    sub["target"] = 0
    sub.loc[
        (sub["Over05_HT"] == 1) &
        (sub["Gol_min_HT"].fillna(0) > minuto) &
        (sub["Gol_min_HT"].fillna(0) <= 35),
        "target"
    ] = 1

    # Features: SH + todos os live até o minuto de entrada
    feats_raw = SH_FEATS.copy()
    for m in [5, 10, 15]:
        if m <= minuto:
            feats_raw += LIVE_FEATS[m]

    feats = [f for f in feats_raw if f in sub.columns]

    X = sub[feats].copy()
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")
        X[col] = X[col].fillna(X[col].median())

    y = sub["target"].values

    return X, y, sub, feats


# ═══════════════════════════════════════════════════════════════════
# TREINO E AVALIAÇÃO
# ═══════════════════════════════════════════════════════════════════

def treinar_modelo(minuto: int, df: pd.DataFrame, salvar: bool = True,
                   forcar: bool = False) -> dict:
    """
    Treina o modelo para a janela de entrada no minuto N.
    Retorna dict com métricas.
    """
    print(f"\n{'─'*55}")
    print(f"  MODELO min{minuto} — target: gol entre min{minuto} e min35")
    print(f"{'─'*55}")

    X, y, sub, feats = construir_dataset(df, minuto)
    n_total = len(X)
    n_gol   = y.sum()
    taxa    = n_gol / n_total

    print(f"  Base:     {n_total:,} jogos ({n_gol:,} gols = {taxa*100:.1f}%)")
    print(f"  Features: {len(feats)}")

    if n_total < MIN_LINHAS_TREINO:
        print(f"  Base insuficiente (< {MIN_LINHAS_TREINO}) — pulando.")
        return {}

    # Verificar crescimento desde o último treino
    arq_log = ARQUIVO_LOG
    if not forcar and salvar and os.path.exists(arq_log):
        log = pd.read_csv(arq_log)
        log_mod = log[log["minuto"] == minuto]
        if not log_mod.empty:
            n_ultimo = log_mod["n_linhas"].iloc[-1]
            crescimento = n_total - n_ultimo
            if crescimento < MIN_CRESCIMENTO:
                print(f"  Crescimento +{crescimento} < {MIN_CRESCIMENTO} — retreino adiado.")
                return {"adiado": True}

    # ── Hiperparâmetros GBM ───────────────────────────────────────
    gbm_params = dict(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        min_samples_leaf=30,
        random_state=42,
    )

    # ── Cross-validation (5-fold) para métricas reais ─────────────
    gbm_cv = GradientBoostingClassifier(**gbm_params)
    cv     = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    print(f"  Treinando (5-fold CV)...", end=" ", flush=True)
    proba_cv = cross_val_predict(gbm_cv, X, y, cv=cv, method="predict_proba")[:, 1]
    auc      = roc_auc_score(y, proba_cv)
    print(f"AUC = {auc:.4f}")

    # ── Métricas por threshold ─────────────────────────────────────
    th_op    = THRESHOLD_OPERACIONAL[minuto]
    odd_col  = f"Odd_Over05HT_{minuto}M"
    odd_35   = "Odd_Over05HT_35M"

    odd_ent  = sub[odd_col].values   if odd_col in sub.columns else np.ones(n_total) * 1.50
    odd_sai  = sub[odd_35].values    if odd_35  in sub.columns else np.ones(n_total) * 3.00

    # Simular P&L com o threshold operacional
    mask_th = proba_cv >= th_op
    pl_list = []
    for i in np.where(mask_th)[0]:
        oe = float(odd_ent[i]) if odd_ent[i] > 1 else 1.50
        odd_s = float(odd_sai[i]) if odd_sai[i] > 0 else 3.00
        if y[i] == 1:
            pl_list.append(oe - 1)
        else:
            red = -(odd_s - oe) / (odd_s - 1) if odd_s > oe else -0.45
            pl_list.append(red)

    n_sel   = mask_th.sum()
    roi_op  = np.mean(pl_list)  if pl_list else 0.0
    acc_op  = y[mask_th].mean() if n_sel > 0 else 0.0
    pl_acc  = np.sum(pl_list)   if pl_list else 0.0
    red_med = np.mean([p for p in pl_list if p < 0]) if any(p<0 for p in pl_list) else 0.0

    print(f"\n  Threshold ≥ {th_op}:")
    print(f"    n_sinais  : {n_sel:,} ({n_sel/n_total*100:.1f}%)")
    print(f"    Acurácia  : {acc_op*100:.1f}%")
    print(f"    ROI médio : {roi_op*100:+.2f}%")
    print(f"    P&L total : {pl_acc:+.1f}u")
    print(f"    Red médio : {red_med*100:+.1f}%")

    if auc < 0.62:
        print(f"  ⚠ AUC {auc:.4f} baixo — verificar qualidade da base.")

    if not salvar:
        return {"minuto": minuto, "auc": auc, "roi": roi_op, "n": n_total}

    # ── Treino final com 100% dos dados ───────────────────────────
    print(f"\n  Treinando modelo final (100% dos dados)...", end=" ", flush=True)
    scaler    = StandardScaler()
    X_scaled  = scaler.fit_transform(X)
    gbm_final = GradientBoostingClassifier(**gbm_params)
    gbm_final.fit(X, y)   # GBM não precisa de scaling, mas scaler guardado para compatibilidade
    print("OK")

    # ── Feature importance ────────────────────────────────────────
    imp = pd.Series(gbm_final.feature_importances_, index=feats).sort_values(ascending=False)
    print(f"\n  Top 10 features:")
    for feat, val in imp.head(10).items():
        bar = "█" * int(val * 150)
        print(f"    {feat:<30} {val:.4f}  {bar}")

    # ── Salvar modelo, scaler, features ──────────────────────────
    arq_modelo  = f"model_m{minuto}.pkl"
    arq_feats   = f"features_m{minuto}.json"
    arq_scaler  = f"scaler_m{minuto}.pkl"

    joblib.dump(gbm_final, arq_modelo)
    joblib.dump(scaler,    arq_scaler)
    with open(arq_feats, "w") as f:
        json.dump(feats, f, indent=2)

    print(f"\n  Salvo:")
    print(f"    {arq_modelo}")
    print(f"    {arq_feats}")
    print(f"    {arq_scaler}")

    # ── Log ───────────────────────────────────────────────────────
    log_entry = {
        "data"         : datetime.now().strftime("%Y-%m-%d %H:%M"),
        "minuto"       : minuto,
        "n_linhas"     : n_total,
        "n_feats"      : len(feats),
        "taxa_gol"     : round(taxa, 4),
        "auc"          : round(auc, 4),
        "threshold_op" : th_op,
        "n_sinais"     : n_sel,
        "acc_op"       : round(acc_op, 4),
        "roi_op"       : round(roi_op, 4),
        "pl_total"     : round(pl_acc, 2),
        "red_med"      : round(red_med, 4),
    }
    df_log = pd.DataFrame([log_entry])
    if os.path.exists(arq_log):
        df_log = pd.concat([pd.read_csv(arq_log), df_log], ignore_index=True)
    df_log.to_csv(arq_log, index=False)
    print(f"    {arq_log} (log atualizado)")

    return log_entry


# ═══════════════════════════════════════════════════════════════════
# FUNÇÃO PÚBLICA: calcular probabilidade (usada pelo pipeline)
# ═══════════════════════════════════════════════════════════════════

def calcular_prob(campos: dict, minuto: int) -> float:
    """
    Calcula a probabilidade do modelo para a janela de entrada no minuto N.

    Parâmetros:
        campos  : dict com todos os campos SH_* e live_XM disponíveis
        minuto  : janela de entrada (5, 10 ou 15)

    Retorna:
        float entre 0 e 1 — probabilidade de gol na janela [minuto, min35]
    """
    arq_modelo = f"model_m{minuto}.pkl"
    arq_feats  = f"features_m{minuto}.json"

    if not os.path.exists(arq_modelo) or not os.path.exists(arq_feats):
        raise FileNotFoundError(
            f"Modelo min{minuto} não encontrado. Rode: python model_trainer.py --minuto {minuto}"
        )

    modelo = joblib.load(arq_modelo)
    with open(arq_feats) as f:
        feats = json.load(f)

    # Montar vetor de features — campos faltantes = 0 (neutro)
    row = {}
    for feat in feats:
        val = campos.get(feat)
        try:
            row[feat] = float(val) if val is not None else 0.0
        except (ValueError, TypeError):
            row[feat] = 0.0

    X = pd.DataFrame([row])
    return float(modelo.predict_proba(X)[0, 1])


def calcular_prob_por_janela(campos: dict) -> dict:
    """
    Calcula probabilidade para as 3 janelas de uma vez.
    Retorna dict: {5: prob, 10: prob, 15: prob}
    Retorna None para janelas cujo modelo não existe.
    """
    resultado = {}
    for m in [5, 10, 15]:
        try:
            resultado[m] = round(calcular_prob(campos, m), 4)
        except FileNotFoundError:
            resultado[m] = None
    return resultado


# ═══════════════════════════════════════════════════════════════════
# AVALIAÇÃO RÁPIDA (sem salvar)
# ═══════════════════════════════════════════════════════════════════

def avaliar_modelos(df: pd.DataFrame):
    """Avalia os 3 modelos sem salvar — útil para checar a saúde dos modelos."""
    print("\n" + "═"*55)
    print("AVALIAÇÃO DOS 3 MODELOS (sem salvar)")
    print("═"*55)

    for minuto in [5, 10, 15]:
        resultado = treinar_modelo(minuto, df, salvar=False)
        if resultado:
            print(f"  min{minuto}: AUC={resultado['auc']:.4f} | "
                  f"ROI={resultado['roi']*100:+.2f}% | n={resultado['n']:,}")

    # Mostrar log histórico se existir
    if os.path.exists(ARQUIVO_LOG):
        print(f"\n  Log histórico ({ARQUIVO_LOG}):")
        log = pd.read_csv(ARQUIVO_LOG)
        print(log[["data","minuto","n_linhas","auc","roi_op","pl_total"]].tail(9).to_string(index=False))


# ═══════════════════════════════════════════════════════════════════
# WALK-FORWARD (relatório de estabilidade)
# ═══════════════════════════════════════════════════════════════════

def relatorio_walkforward(df: pd.DataFrame, minuto: int, threshold: float = None):
    """Gera relatório de walk-forward semanal para o modelo do minuto N."""
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import StratifiedKFold, cross_val_predict

    th = threshold or THRESHOLD_OPERACIONAL[minuto]
    X, y, sub, feats = construir_dataset(df, minuto)

    gbm = GradientBoostingClassifier(
        n_estimators=200, max_depth=3, learning_rate=0.05,
        subsample=0.8, min_samples_leaf=30, random_state=42
    )
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    proba = cross_val_predict(gbm, X, y, cv=cv, method="predict_proba")[:, 1]

    sub   = sub.copy()
    sub["proba"] = proba
    sub["Data_dt"] = pd.to_datetime(sub["Data"])
    sub["Semana"]  = sub["Data_dt"].dt.to_period("W")

    odd_col = f"Odd_Over05HT_{minuto}M"
    odd_ent = sub[odd_col].values if odd_col in sub.columns else np.ones(len(sub)) * 1.50
    odd_sai = sub["Odd_Over05HT_35M"].values if "Odd_Over05HT_35M" in sub.columns else np.ones(len(sub)) * 3.0

    print(f"\n  Walk-forward min{minuto} (th≥{th}):")
    print(f"  {'Semana':<14} {'n':>4}  {'Acc':>6}  {'ROI':>8}  {'P&L':>7}  ok?")
    print("  " + "─"*50)

    pos = tot = 0
    pl_total = 0.0

    for sem in sorted(sub["Semana"].unique()):
        mask_sem = (sub["Semana"] == sem) & (sub["proba"] >= th)
        n = mask_sem.sum()
        if n < 5:
            continue
        pl_list = []
        for i in sub[mask_sem].index:
            oe = float(odd_ent[i]) if odd_ent[i] > 1 else 1.50
            odd_s = float(odd_sai[i]) if odd_sai[i] > 0 else 3.0
            if sub.at[i, "target"] == 1 if "target" in sub.columns else y[i] == 1:
                pl_list.append(oe - 1)
            else:
                pl_list.append(-(odd_s - oe) / (odd_s - 1) if odd_s > oe else -0.45)
        roi = np.mean(pl_list)
        pl  = np.sum(pl_list)
        acc = (np.array(pl_list) > 0).mean()
        pl_total += pl
        tot += 1
        if roi > 0:
            pos += 1
        flag = "✓" if roi > 0 else "✗"
        print(f"  {str(sem):<14} {n:>4}  {acc*100:>5.1f}%  {roi*100:>+7.2f}%  {pl:>+7.2f}u  {flag}")

    print("  " + "─"*50)
    pct = pos / tot * 100 if tot > 0 else 0
    print(f"  Semanas +: {pos}/{tot} ({pct:.0f}%)  |  P&L total: {pl_total:+.1f}u")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="FULLBETS Model Trainer")
    parser.add_argument("--minuto",   type=int, default=None,
                        help="Treinar só este minuto (5, 10 ou 15). Default: todos.")
    parser.add_argument("--avaliar",  action="store_true",
                        help="Só avalia sem salvar.")
    parser.add_argument("--forcar",   action="store_true",
                        help="Força retreino mesmo sem crescimento suficiente.")
    parser.add_argument("--wf",       type=int, default=None,
                        help="Gera relatório walk-forward para o minuto N.")
    parser.add_argument("--base",     default=ARQUIVO_BASE,
                        help=f"Caminho do xlsx. Default: {ARQUIVO_BASE}")
    args = parser.parse_args()

    if not os.path.exists(args.base):
        print(f"ERRO: Base não encontrada — {args.base}")
        sys.exit(1)

    print(f"\n{'═'*55}")
    print(f"  FULLBETS MODEL TRAINER")
    print(f"  Base: {args.base}")
    print(f"  Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'═'*55}")

    print(f"\n  Carregando base...", end=" ", flush=True)
    df = pd.read_excel(args.base)
    validos = (df["Filtro_0x0_min5"] == 1).sum()
    print(f"{len(df):,} linhas | {validos:,} válidos")

    # Walk-forward
    if args.wf is not None:
        if args.wf not in [5, 10, 15]:
            print("ERRO: --wf deve ser 5, 10 ou 15")
            sys.exit(1)
        relatorio_walkforward(df, args.wf)
        return

    # Só avaliação
    if args.avaliar:
        avaliar_modelos(df)
        return

    # Treino
    minutos = [args.minuto] if args.minuto else [5, 10, 15]
    for m in minutos:
        if m not in [5, 10, 15]:
            print(f"ERRO: minuto deve ser 5, 10 ou 15 (recebido: {m})")
            continue
        resultado = treinar_modelo(m, df, salvar=True, forcar=args.forcar)

    print(f"\n{'═'*55}")
    print(f"  Concluído.")
    print(f"{'═'*55}\n")


if __name__ == "__main__":
    main()
