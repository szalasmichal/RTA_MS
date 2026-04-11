from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

# Konfiguracja Konsumenta (czyta z 'transactions')
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Konfiguracja Producenta (wysyła do 'alerts')
alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    
    # R1: kwota > 3000 (+3)
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
        
    # R2: elektronika i kwota > 1500 (+2)
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
        
    # R3: godzina < 6 (noc) (+2)
    tx_time = datetime.fromisoformat(tx['timestamp'])
    if tx_time.hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

print("System monitoringu uruchomiony...")

for message in consumer:
    tx = message.value
    
    # obliczanie scoringu
    total_score, broken_rules = score_transaction(tx)
    
    # Jeśli transakcja jest podejrzana (score >= 3)
    if total_score >= 3:
        # Wzbogacamy dane o wyniki analizy
        tx['total_score'] = total_score
        tx['broken_rules'] = broken_rules
        
        # Producent 'alert_producer' wysyła informację do nowego tematu Kafki
        alert_producer.send('alerts', value=tx)
        
        # Wypisujemy powiadomienie w konsoli
        print(f"ALERT! Podejrzana transakcja {tx['tx_id']}: {total_score} pkt. Reguły: {broken_rules}")
