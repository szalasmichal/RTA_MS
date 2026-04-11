from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Konsument - analiza ryzyka")

for message in consumer:
    tx = message.value

    if tx['amount'] > 3000:
        risk_level = "HIGH"
    elif tx['amount'] > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    # nowe pole do słownika
    tx['risk_level'] = risk_level
    
    # Wyświetlamy efekt
    color = "🔴" if risk_level == "HIGH" else ("🟡" if risk_level == "MEDIUM" else "🟢")
    print(f"{color} [{risk_level}] ID: {tx['tx_id']} | Kwota: {tx['amount']:.2f} PLN | Sklep: {tx['store']}")
