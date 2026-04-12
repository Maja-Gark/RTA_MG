from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict

# Konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Słownik do przechowywania historii timestampów per użytkownik
# Klucz: user_id, Wartość: lista obiektów datetime
user_windows = defaultdict(list)

print("System detekcji anomalii prędkości uruchomiony...")

try:
    for message in consumer:
        tx = message.value
        
        # --- TWÓJ KOD START ---
        user_id = tx['user_id']
        current_time = datetime.fromisoformat(tx['timestamp'])
        
        # 1. Dodajemy aktualny znacznik czasu do listy użytkownika
        user_windows[user_id].append(current_time)
        
        # 2. Czyścimy okno: usuwamy wpisy starsze niż 60 sekund względem obecnej transakcji
        # To kluczowy element "przesuwania" okna
        cutoff = current_time - timedelta(seconds=60)
        user_windows[user_id] = [ts for ts in user_windows[user_id] if ts > cutoff]
        
        # 3. Sprawdzenie warunku (więcej niż 3 transakcje w oknie)
        count = len(user_windows[user_id])
        if count > 3:
            print(f"⚠️  ALERT: Wykryto anomalię prędkości!")
            print(f"   Użytkownik {user_id} wykonał {count} transakcje w ciągu ostatnich 60s.")
            print(f"   Ostatnia transakcja: {tx['tx_id']} | Sklep: {tx['store']} | Kwota: {tx['amount']} PLN")
            print("-" * 50)
        # --- TWÓJ KOD KONIEC ---

except KeyboardInterrupt:
    print("\nZamykanie systemu detekcji...")
finally:
    consumer.close()
