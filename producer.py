import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

TOPICO = "transacoes"
SERVIDOR_KAFKA = "localhost:9092"
INTERVALO_SEGUNDOS = 3

CIDADES = ["Fortaleza", "Recife", "Salvador", "São Paulo", "Curitiba"]

CLIENTES = {
    "C001": "Recife",
    "C002": "Salvador",
    "C003": "São Paulo",
    "C004": "Curitiba",
    "C005": "Fortaleza",
    "C006": "Recife",
    "C007": "Salvador",
    "C008": "São Paulo",
    "C009": "Curitiba",
    "C010": "Fortaleza",
}

CATEGORIAS = ["supermercado", "farmacia", "restaurante", "eletronicos", "viagem"]


def gerar_transacao():
    cliente = random.choice(list(CLIENTES))

    if random.random() < 0.95:
        cidade = CLIENTES[cliente]
    else:
        cidade = random.choice(CIDADES)

    if random.random() < 0.10:
        valor = round(random.uniform(10000, 20000), 2)
    else:
        valor = round(random.uniform(10, 9000), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": cliente,
        "amount": valor,
        "city": cidade,
        "category": random.choice(CATEGORIAS),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def main():
    produtor = KafkaProducer(
        bootstrap_servers=SERVIDOR_KAFKA,
        value_serializer=lambda dados: json.dumps(dados).encode("utf-8"),
    )
    print(f"Producer iniciado. Enviando para o tópico '{TOPICO}' a cada {INTERVALO_SEGUNDOS}s...")
    try:
        while True:
            transacao = gerar_transacao()
            produtor.send(TOPICO, transacao)
            produtor.flush()
            print(f"[ENVIADA] {transacao}")
            time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("Encerrando producer.")
    finally:
        produtor.close()


if __name__ == "__main__":
    main()