from kafka import KafkaConsumer
import json

# Inicjalizacja konsumenta z unikalną grupą (group_id)
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',  # Czyta od początku, jeśli grupa jest nowa
    group_id='enrichment-service-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono proces wzbogacania transakcji (Risk Leveling)...")

try:
    for message in consumer:
        tx = message.value
        
        # Logika wyznaczania poziomu ryzyka
        if tx['amount'] > 3000:
            tx['risk_level'] = 'HIGH'
        elif tx['amount'] > 1000:
            tx['risk_level'] = 'MEDIUM'
        else:
            tx['risk_level'] = 'LOW'
            
        # Wypisanie wzbogaconej transakcji
        print(f"ID: {tx['tx_id']} | Kwota: {tx['amount']:>8.2f} | Ryzyko: {tx['risk_level']}")
        
except KeyboardInterrupt:
    print("\nZamykanie konsumenta...")
finally:
    consumer.close()
