from kafka import KafkaConsumer
from collections import Counter
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='counting-service-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Rozpoczynanie zliczania transakcji...")

try:
    for message in consumer:
        tx = message.value
        store = tx['store']
        amount = tx['amount']
        
        # 1. Zwiększenie liczników
        store_counts[store] += 1
        total_amount[store] = total_amount.get(store, 0) + amount
        msg_count += 1
        
        # 2. Raportowanie co 10 wiadomości
        if msg_count % 10 == 0:
            print(f"\n--- RAPORT PO {msg_count} WIADOMOŚCIACH ---")
            print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<10} | {'Średnia':<10}")
            print("-" * 50)
            
            # Sortowanie po liczbie transakcji (malejąco)
            for s in sorted(store_counts.keys()):
                count = store_counts[s]
                total = total_amount[s]
                avg = total / count
                print(f"{s:<12} | {count:<7} | {total:>8.2f} | {avg:>8.2f}")
            print("-" * 50)


except KeyboardInterrupt:
    print("\nZatrzymywanie konsumenta...")
finally:
    consumer.close()
