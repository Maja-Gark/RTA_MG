from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_windows = defaultdict(list)

print("System detekcji anomalii prędkości uruchomiony...")

try:
    for message in consumer:
        tx = message.value
        
        user_id = tx['user_id']
        current_time = datetime.fromisoformat(tx['timestamp'])
        
        user_windows[user_id].append(current_time)
        
        cutoff = current_time - timedelta(seconds=60)
        user_windows[user_id] = [ts for ts in user_windows[user_id] if ts > cutoff]
        
        count = len(user_windows[user_id])
        if count > 3:
            print(f"   ALERT: Wykryto anomalię prędkości!")
            print(f"   Użytkownik {user_id} wykonał {count} transakcje w ciągu ostatnich 60s.")
            print(f"   Ostatnia transakcja: {tx['tx_id']} | Sklep: {tx['store']} | Kwota: {tx['amount']} PLN")
            print("-" * 50)

except KeyboardInterrupt:
    print("\nZamykanie systemu detekcji...")
finally:
    consumer.close()
