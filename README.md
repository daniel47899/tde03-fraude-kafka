# TDE 03 - Detecção de Fraude com Apache Kafka

## Arquitetura
Producer -> Kafka (tópico `transacoes`) -> Consumer -> SQLite (`transacoes.db`) + alertas no console

## Formato da transação (JSON)
```json
{
  "transaction_id": "uuid",
  "customer_id": "C001",
  "amount": 1234.56,
  "city": "Fortaleza",
  "category": "farmacia",
  "timestamp": "2026-10-16T14:30:00"
}
```

## Regras de fraude
| Código | Regra |
|---|---|
| ALTO_VALOR | amount >= 10000 |
| TEMPO_60s | mesmo cliente com 4 transações em < 60s |
| GEO_10m | mesmo cliente com transações em cidades diferentes em 10 min |

## Como executar
```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python consumer.py     # terminal 1
python producer.py     # terminal 2
```

## Consultar o banco
```bash
sqlite3 transacoes.db "SELECT * FROM transacoes WHERE is_fraud=1;"
```
