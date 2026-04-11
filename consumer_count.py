from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()          # ilość transakcji
total_amount = defaultdict(float) # wartość transakcji
msg_count = 0                     # ilość przetworzonych wiadomości

print("Konsument agregujący")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    # Raportowanie co 10 wiadomości
    if msg_count % 10 == 0:
        print(f"\n Stan po {msg_count} transakcjach")
        print(f"{'Sklep':<12} | {'Ilość':<6} | {'Suma (PLN)':<12}")
        print("-" * 30)
        
        # Pętla po wszystkich sklepach
        for s in store_counts:
            print(f"{s:<12} | {store_counts[s]:<6} | {total_amount[s]:.2f}")
