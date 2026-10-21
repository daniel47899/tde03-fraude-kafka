import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
 
from kafka import KafkaConsumer
 
TOPICO = "transacoes"
SERVIDOR_KAFKA = "localhost:9092"
ARQUIVO_BANCO = "transacoes.db"
 
VALOR_MINIMO_ALTO_VALOR = 10000
QUANTIDADE_TRANSACOES_TEMPO = 4
JANELA_TEMPO = timedelta(seconds=60)
JANELA_GEO = timedelta(minutes=10)
 
historico_por_cliente = defaultdict(list)
 
 
def criar_banco():
    conexao_banco = sqlite3.connect(ARQUIVO_BANCO)
    conexao_banco.execute(
        """
        CREATE TABLE IF NOT EXISTS transacoes (
            transaction_id TEXT PRIMARY KEY,
            customer_id    TEXT NOT NULL,
            amount         REAL NOT NULL,
            city           TEXT NOT NULL,
            category       TEXT,
            timestamp      TEXT NOT NULL,
            is_fraud       INTEGER NOT NULL,
            fraud_types    TEXT
        )
        """
    )
    conexao_banco.commit()
    return conexao_banco
 
 
def detectar_fraudes(transacao, horario_transacao):
    fraudes_encontradas = []
    cliente = transacao["customer_id"]
    compras_anteriores = historico_por_cliente[cliente]
 
    if transacao["amount"] >= VALOR_MINIMO_ALTO_VALOR:
        fraudes_encontradas.append("ALTO_VALOR")
 
    quantidade_compras_ultimos_60s = 0
    for horario_compra, _ in compras_anteriores:
        if horario_transacao - horario_compra < JANELA_TEMPO:
            quantidade_compras_ultimos_60s += 1
 
    if quantidade_compras_ultimos_60s + 1 >= QUANTIDADE_TRANSACOES_TEMPO:
        fraudes_encontradas.append("TEMPO_60s")
 
    for horario_compra, cidade_compra in compras_anteriores:
        foi_ha_menos_de_10min = horario_transacao - horario_compra < JANELA_GEO
        if foi_ha_menos_de_10min and cidade_compra != transacao["city"]:
            fraudes_encontradas.append("GEO_10m")
            break
 
    compras_anteriores.append((horario_transacao, transacao["city"]))
 
    return fraudes_encontradas
 
 
def salvar_transacao(conexao_banco, transacao, fraudes_encontradas):
    conexao_banco.execute(
        "INSERT OR REPLACE INTO transacoes VALUES (?,?,?,?,?,?,?,?)",
        (
            transacao["transaction_id"],
            transacao["customer_id"],
            transacao["amount"],
            transacao["city"],
            transacao.get("category"),
            transacao["timestamp"],
            1 if fraudes_encontradas else 0,
            ",".join(fraudes_encontradas) if fraudes_encontradas else None,
        ),
    )
    conexao_banco.commit()
 
 
def exibir_transacao(transacao, fraudes_encontradas):
    if fraudes_encontradas:
        print("=" * 60)
        print(f"🚨 FRAUDE DETECTADA: {', '.join(fraudes_encontradas)}")
    else:
        print("-" * 60)
        print("✅ Transação normal")
    print(f"  ID:      {transacao['transaction_id']}")
    print(f"  Cliente: {transacao['customer_id']}")
    print(f"  Valor:   R$ {transacao['amount']:.2f}")
    print(f"  Cidade:  {transacao['city']}")
    print(f"  Data:    {transacao['timestamp']}")
    if fraudes_encontradas:
        print("=" * 60)
 
 
def main():
    conexao_banco = criar_banco()
    consumidor = KafkaConsumer(
        TOPICO,
        bootstrap_servers=SERVIDOR_KAFKA,
        group_id="detector-fraude",
        auto_offset_reset="earliest",
        value_deserializer=lambda bytes_recebidos: json.loads(bytes_recebidos.decode("utf-8")),
    )
    print(f"Consumer iniciado. Lendo o tópico '{TOPICO}'...")
    try:
        for mensagem in consumidor:
            transacao = mensagem.value
            horario_transacao = datetime.fromisoformat(transacao["timestamp"])
 
            fraudes_encontradas = detectar_fraudes(transacao, horario_transacao)
            salvar_transacao(conexao_banco, transacao, fraudes_encontradas)
            exibir_transacao(transacao, fraudes_encontradas)
    except KeyboardInterrupt:
        print("Encerrando consumer.")
    finally:
        consumidor.close()
        conexao_banco.close()
 
 
if __name__ == "__main__":
    main()