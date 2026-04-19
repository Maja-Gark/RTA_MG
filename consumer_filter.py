from kafka import KafkaConsumer
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    # Pobranie słownika z danymi transakcji
    tx = message.value
    # Sprawdzenie warunku kwoty transakcji
    if tx['amount'] > 3000:
        print(f"ALERT: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
