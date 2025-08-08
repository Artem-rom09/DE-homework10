# Потокова обробка даних з Wikimedia за допомогою Spark, Kafka та Cassandra

Цей проєкт демонструє повний цикл обробки даних у реальному часі. Він отримує потік подій про створення сторінок у Wikimedia, обробляє його за допомогою Spark Streaming, передає через Kafka і зберігає відфільтровані результати в базі даних Cassandra.

---

## Архітектура та Технології

* **Оркестрація:** Docker & Docker Compose
* **Брокер повідомлень:** Apache Kafka
* **Потокова обробка:** Apache Spark (Spark Streaming)
* **База даних:** Apache Cassandra
* **Мова програмування:** Python

---

## Структура Проєкту

```
/spark-streaming-wikipedia/
|
|-- docker-compose.yml      # Запускає всю інфраструктуру
|-- schema.cql              # Схема для таблиці в Cassandra
|-- README.md               # Цей файл
|
|-- generator/              # Код для отримання даних з Wikimedia та відправки в Kafka
|   |-- generator.py
|   |-- requirements.txt
|   `-- Dockerfile
|
`-- spark-apps/             # Код для двох Spark-додатків
    |-- processor.py        # 1. Читає з Kafka, фільтрує, пише в Kafka
    |-- writer.py           # 2. Читає з Kafka, пише в Cassandra
    `-- requirements.txt
```

---

## Покрокова інструкція по запуску

### 1. Вимоги
- Встановлений **Docker** та **Docker Compose**.

### 2. Запуск інфраструктури
Відкрийте термінал у кореневій папці проєкту та виконайте команду. Вона побудує образ для генератора та запустить усі 6 контейнерів у фоновому режимі.
```bash
docker-compose up --build -d
```
> **Зачекайте 1-2 хвилини**, поки всі сервіси стабілізуються, особливо Kafka та Cassandra.

### 3. Підготовка бази даних
Ця команда виконає скрипт `schema.cql` і створить у Cassandra необхідний простір ключів та таблицю.
```bash
docker exec -i cassandra cqlsh < schema.cql
```

### 4. Встановлення залежностей для Spark
Щоб наші Spark-додатки могли працювати, потрібно один раз встановити для них `pyspark`.
```bash
docker exec -it spark-master pip install -r /opt/bitnami/spark/apps/requirements.txt
```

### 5. Запуск Spark-додатків
Тепер надішлемо наші два скрипти на виконання у Spark-кластер. Вони будуть працювати у фоновому режимі.

* **Запуск процесора (фільтрація даних):**
    ```bash
    docker exec -d spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/apps/processor.py
    ```
* **Запуск записувача (збереження в Cassandra):**
    ```bash
    docker exec -d spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /opt/bitnami/spark/apps/writer.py
    ```
**Готово!** Ваш конвеєр даних працює. Дайте йому попрацювати 3-5 хвилин, щоб накопичити дані.

---

## Перевірка результатів

Ви можете перевірити кожен етап конвеєра за допомогою цих команд.

* **Вміст вхідного топіку `input` (сирі дані):**
    ```bash
    docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic input --max-messages 3
    ```

* **Вміст обробленого топіку `processed` (відфільтровані дані):**
    ```bash
    docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic processed --max-messages 3
    ```

* **Дані в таблиці Cassandra:**
    ```bash
    docker exec -it cassandra cqlsh -e "SELECT * FROM wikipedia.page_creations LIMIT 5;"
    ```

---

## Зупинка системи

Щоб зупинити всі контейнери та видалити пов'язані з ними ресурси (включно з томами даних), виконайте:
```bash
docker-compose down --volumes
```
