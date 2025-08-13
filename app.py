import os
import io
import time
import math
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import streamlit as st
from datetime import datetime
from pandas.tseries.offsets import DateOffset

st.set_page_config(
    page_title="Monitor B3 • 12 semanas",
    page_icon="📈",
    layout="wide"
)

# -----------------------------
# Configurações e utilidades
# -----------------------------
DEFAULT_LOOKBACK_WEEKS = 12
DEFAULT_MIN_RETURN = 30.0  # %
DEFAULT_BATCH_SIZE = 100

@st.cache_data(ttl=3600)
def get_b3_tickers_from_brapi():
    """
    Tenta obter todos os tickers de ações brasileiras via brapi.dev.
    Retorna lista como ['PETR4.SA', 'VALE3.SA', ...]
    """
    try:
        url = "https://brapi.dev/api/quote/list?limit=10000"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        # Estrutura esperada: {"stocks":[{"stock":"PETR4","name":"...","type":"stock"}, ...]}
        stocks = data.get("stocks", [])
        tickers = []
        for s in stocks:
            code = s.get("stock")
            asset_type = s.get("type")
            # Mantém apenas ações ("stock"). Exclui fundos, ETFs, BDRs etc.
            if code and isinstance(code, str) and (asset_type == "stock"):
                # Exclui tickers com sufixos indesejados (ex.: 'F' de fracionário)
                # e preferencialmente apenas códigos com 3 ou 4 no final (ON/PN mais comuns)
                if code.endswith(("3", "4", "5", "6", "7", "8", "11")):
                    tickers.append(code.strip().upper() + ".SA")
        # Remove duplicados e ordena
        tickers = sorted(list(set(tickers)))
        return tickers
    except Exception as e:
        st.warning(f"Falha ao obter tickers via brapi.dev (usando fallback): {e}")
        return None

@st.cache_data(ttl=24*3600)
def get_b3_tickers():
    """
    Obtém tickers de ações B3. Primeiro tenta via brapi.
    Se falhar, usa tickers_fallback.csv do repositório.
    """
    tickers = get_b3_tickers_from_brapi()
    if tickers:
        return tickers

    # Fallback: lê CSV local
    try:
        df = pd.read_csv("tickers_fallback.csv")
        codes = df["ticker"].dropna().astype(str).str.upper().tolist()
        # Garante sufixo .SA
        codes = [c if c.endswith(".SA") else c + ".SA" for c in codes]
        codes = sorted(list(set(codes)))
        st.info("Usando lista de fallback de tickers (tickers_fallback.csv).")
        return codes
    except Exception as e:
        st.error("Não foi possível obter a lista de tickers (brapi e fallback falharam).")
        st.stop()

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def choose_price_frame(df):
    """
    Recebe DataFrame de yfinance.download com MultiIndex de colunas.
    Retorna o frame de preços preferindo 'Adj Close' se disponível, senão 'Close'.
    """
    lvl0 = df.columns.get_level_values(0)
    if "Adj Close" in set(lvl0):
        return df["Adj Close"]
    return df["Close"]

def compute_return_series(series: pd.Series, weeks: int):
    """
    Dada uma série de preços (por ticker), calcula a variação entre a última
    cotação disponível e a cotação de 'weeks' semanas atrás (ajustada para o
    dia útil anterior se necessário). Retorna (pct, last_date, ref_date, last_price, ref_price).
    """
    s = series.dropna()
    if s.empty:
        return None

    last_date = s.index.max()
    ref_target = last_date - DateOffset(weeks=weeks)
    s_ref = s.loc[:ref_target]
    if s_ref.empty:
        return None

    ref_date = s_ref.index.max()
    last_price = float(s.loc[last_date])
    ref_price = float(s_ref.loc[ref_date])
    if ref_price <= 0:
        return None

    pct = (last_price / ref_price) - 1.0
    return pct, last_date, ref_date, last_price, ref_price

@st.cache_data(ttl=1800, show_spinner=False)
def download_prices(tickers, period="6mo", interval="1d"):
    """
    Baixa preços em lotes para reduzir falhas e tempo.
    Retorna DataFrame concatenado em MultiIndex [field, ticker].
    """
    results = []
    failures = []
    for batch in chunk_list(tickers, DEFAULT_BATCH_SIZE):
        try:
            df = yf.download(
                batch,
                period=period,
                interval=interval,
                auto_adjust=False,
                threads=True,
                progress=False
            )
            # Caso apenas 1 ticker retorne, yfinance traz colunas simples; vamos padronizar
            if isinstance(df.columns, pd.Index):
                # single ticker; precisamos tornar MultiIndex
                # suposição: colunas são ['Open','High','Low','Close','Adj Close','Volume']
                ticker = batch[0]
                df = pd.concat({ticker: df}, axis=1).swaplevel(0,1, axis=1)
            results.append(df)
        except Exception as e:
            failures.extend(batch)

    if not results:
        raise RuntimeError("Falha ao baixar preços de todos os lotes.")

    full = pd.concat(results, axis=1).sort_index(axis=1)
    return full, failures

def filter_variation(prices_df, weeks, min_return_pct, suffix_strip=True):
    """
    prices_df: DataFrame com colunas MultiIndex (field, ticker)
    Retorna DataFrame com variações e informações auxiliares, filtrado por min_return_pct.
    """
    price_frame = choose_price_frame(prices_df)
    rows = []
    for ticker in price_frame.columns:
        res = compute_return_series(price_frame[ticker], weeks)
        if not res:
            continue
        pct, last_date, ref_date, last_price, ref_price = res
        pct_pct = pct * 100.0
        if pct_pct >= min_return_pct:
            rows.append({
                "ticker": ticker.replace(".SA", "") if suffix_strip else ticker,
                "ret_12w_pct": round(pct_pct, 2),
                "last_close": round(last_price, 4),
                "ref_close": round(ref_price, 4),
                "last_date": last_date.date().isoformat(),
                "ref_date": ref_date.date().isoformat()
            })
    if not rows:
        return pd.DataFrame(columns=["ticker","ret_12w_pct","last_close","ref_close","last_date","ref_date"])
    out = pd.DataFrame(rows).sort_values("ret_12w_pct", ascending=False)
    return out

def csv_download_button(df, filename):
    csv = df.to_csv(index=False, encoding="utf-8")
    st.download_button(
        "Baixar CSV",
        data=csv,
        file_name=filename,
        mime="text/csv"
    )

# -----------------------------
# UI
# -----------------------------
st.title("📈 Monitor B3: altas ≥ 30% nas últimas 12 semanas")
st.caption("Fonte: Yahoo Finance (yfinance). Este app busca todas as ações da B3, calcula a variação em 12 semanas e filtra as com alta ≥ 30%.")

with st.sidebar:
    st.header("Parâmetros")
    lookback_weeks = st.number_input("Período (semanas)", min_value=4, max_value=52, value=DEFAULT_LOOKBACK_WEEKS, step=1)
    min_return = st.number_input("Mínimo de alta (%)", min_value=0.0, max_value=1000.0, value=DEFAULT_MIN_RETURN, step=5.0)
    batch_size = st.slider("Tamanho do lote (para download de preços)", 50, 300, DEFAULT_BATCH_SIZE, step=25)
    st.write("Dica: se estiver lento, diminua o lote. Se estiver estável, aumente.")
    force_refresh = st.button("🔄 Atualizar agora (limpar cache)")

if force_refresh:
    get_b3_tickers.clear()
    get_b3_tickers_from_brapi.clear()
    download_prices.clear()
    st.success("Cache limpo. Os dados serão recarregados.")

st.subheader("1) Universo de ações da B3")
tickers = get_b3_tickers()
st.write(f"Total de tickers candidatos: {len(tickers)}")

if len(tickers) == 0:
    st.error("Nenhum ticker encontrado.")
    st.stop()

# Ajusta batch global conforme sidebar
DEFAULT_BATCH_SIZE = batch_size

st.subheader("2) Coleta de preços (últimos ~6 meses, diário)")
with st.spinner("Baixando cotações... isso pode levar de 20s a 2min dependendo do número de tickers."):
    prices_df, failures = download_prices(tickers, period="6mo", interval="1d")

if failures:
    st.warning(f"Falha ao baixar {len(failures)} tickers. Ex.: {failures[:10]}")

st.subheader("3) Cálculo de variação em 12 semanas e filtro")
with st.spinner("Calculando variações e filtrando..."):
    result = filter_variation(prices_df, weeks=lookback_weeks, min_return_pct=min_return)

st.success(f"Encontradas {len(result)} ações com alta ≥ {min_return:.0f}% em {lookback_weeks} semanas.")

st.dataframe(
    result,
    hide_index=True,
    use_container_width=True
)

csv_download_button(result, f"b3_monitor_{lookback_weeks}w_min{int(min_return)}pct.csv")

with st.expander("Como a variação é calculada?"):
    st.markdown("""
- Pegamos a última cotação disponível (último fechamento).
- Definimos a data de referência como a última data menos N semanas (ex.: 12).
- Usamos a cotação do dia útil anterior ou igual à data de referência (se o dia cair em fim de semana/feriado).
- Variação = (Último / Referência) - 1.
- Exibimos em % (duas casas decimais).
    """)

st.caption("Aviso: apenas para fins informativos/educacionais. Não é recomendação de investimento.")
