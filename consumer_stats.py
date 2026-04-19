from kafka import KafkaConsumer
from collections import defaultdict
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='category-stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Struktura do przechowywania statystyk: count, total, min, max
# Lambda pozwala na automatyczną inicjalizację słownika dla nowej kategorii
stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': 0.0})
msg_count = 0

print("Analiza statystyk per kategoria uruchomiona...")

try:
    for message in consumer:
        tx = message.value
        cat = tx['category']
        amt = tx['amount']
        
        # 1. Aktualizacja metryk dla danej kategorii
        s = stats[cat]
        s['count'] += 1
        s['total'] += amt
        s['min'] = min(s['min'], amt)
        s['max'] = max(s['max'], amt)
        
        msg_count += 1
        
        # 2. Raportowanie co 10 wiadomości
        if msg_count % 10 == 0:
            print(f"\n--- STATYSTYKI KATEGORII (Wiadomości: {msg_count}) ---")
            print(f"{'Kategoria':<12} | {'Sztuk':<5} | {'Suma':>10} | {'Min':>8} | {'Max':>8}")
            print("-" * 55)
            
            for category, data in sorted(stats.items()):
                print(f"{category:<12} | {data['count']:<5} | {data['total']:>10.2f} | {data['min']:>8.2f} | {data['max']:>8.2f}")
            print("-" * 55)

except KeyboardInterrupt:
    print("\nZamykanie konsumenta statystyk...")
finally:
    consumer.close()
