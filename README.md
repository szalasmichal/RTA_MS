# Analiza Danych w Czasie Rzeczywistym - Apache Kafka

System monitorujący transakcje sklepowe pod kątem oszustw w czasie rzeczywistym.

---

## 🛠️ Skład projektu

W repozytorium znajdują się następujące pliki:

* `producer.py` – Generuje losowe transakcje, z czego **5%** jest celowo oznaczonych jako podejrzane.
* `consumer_filter.py` – Konsument filtrujący transakcje o wartości powyżej **1000 PLN**.
* `consumer_enrich.py` – Konsument dodający pole poziomu ryzyka (**LOW**, **MEDIUM**, **HIGH**) na podstawie kwoty.
* `consumer_count.py` – Konsument stanowy, wyliczający statystyki sprzedaży per sklep.
* `scoring_consumer.py` – Konsument scoringowy, oceniający transakcje według zadanego systemu punktowego i generujący alerty.

---

## 🚀 Jak uruchomić?

Aby system zadziałał poprawnie, wykonaj poniższe kroki w podanej kolejności. Każdy program powinien działać w osobnym terminalu.

### 1. Uruchomienie brokera Kafki
Upewnij się, że Twój serwer Kafki (np. w Dockerze) działa poprawnie.

### 2. Uruchomienie konsumentów
W osobnych terminalach uruchom wybranych konsumentów, na przykład:

```bash
python scoring_consumer.py
```

### 3. Uruchomienie producenta
Dopiero gdy konsumenci są gotowi, uruchom program generujący dane:

```bash
python producer.py
```
