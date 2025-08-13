# Monitor B3 • 12 semanas (Streamlit + yfinance)

Este webapp lista ações da B3 que subiram pelo menos X% (padrão: 30%) nos últimos N semanas (padrão: 12), usando dados do Yahoo Finance via yfinance.

## Como funciona
- Universo de tickers: tenta buscar todos via brapi.dev, filtrando somente ações (type=stock). Caso falhe, usa tickers_fallback.csv.
- Coleta de preços: yfinance, período ~6 meses, diário.
- Cálculo: compara o último fechamento com o fechamento do dia útil anterior/igual de N semanas atrás.
- Filtro: mostra apenas as que subiram ≥ limite escolhido (padrão 30%).

## Rodando localmente
1) Crie um ambiente virtual (opcional) e instale:
