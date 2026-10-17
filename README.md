# Отчёт по практической №1

## 1. Вариант и масштаб
| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Фазлыев Эрик Маратович  |
| Вариант (запрос) | Q16 «Parts/Supplier Relationship» |
| Масштаб `sfN` | sf5 |
| Почему выбрал такой масштаб | Q16 использует только `part`, `partsupp` и `supplier`, это самые маленькие таблицы TPC-H. На sf5 в `partsupp` 4 млн строк, в `part` 1 млн. Такого объёма хватает для заметной разницы в замерах, данные помещаются на диск и обрабатываются за секунды.|

## 2. Поднятые сервисы — и почему именно они

| Сервис | Роль в стеке | Почему именно он (а не альтернатива) |
|---|---|---|
| MinIO (форк Silo) | S3-совместимое объектное хранилище, сюда падают parquet-файлы | Поддерживает S3 API, с которым работают Trino и Lakekeeper; открытый код, запускается локально одной командой|
| PostgreSQL | БД метаданных Lakekeeper | Проверенная транзакционная СУБД с открытым кодом, Lakekeeper работает с ней без дополнительной настройки|
| Lakekeeper | Iceberg REST Catalog | Работает по открытому REST-протоколу Iceberg и подходит для любого движка и облака; с каталогом работают и Trino, и Spark|
| Trino | SQL-движок | Работает с Iceberg через REST-каталог и записывает parquet сразу в S3, так что хранение и вычисления разнесены|
| Spark (часть 2) | второй движок поверх Bronze | см. раздел 6.2 |

### Скрипты, которыми поднимал стек
```powershell
#Поднятие контейнера с minIO
docker run -d --name lakehouse-minio -p 9000:9000 -p 9011:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin -v minio-data:/data pgsty/silo:latest server /data --console-address ":9001"

#Создание бакетов для медальонов
docker run --rm --entrypoint /bin/sh pgsty/mc:latest -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && mc mb --ignore-existing m/bronze m/silver m/gold"

#Поднятие постгре
docker run -d --name lakehouse-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -v pg-data:/var/lib/postgresql/data postgres:17

#Миграции БД Lakekeeper
docker run --rm -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' quay.io/lakekeeper/catalog:latest-main migrate

#Поднятие lakekeeper
docker run -d --name lakekeeper -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' -p 8181:8181 quay.io/lakekeeper/catalog:latest-main serve

#Warehouse bronze/silver/gold были созданы через UI

#Создание каталогов в trino (на каждый по файлу .properties и генератору)
foreach ($n in "bronze","silver","gold") {
  (Get-Content catalog\lakekeeper.properties) -replace 'iceberg.rest-catalog.warehouse=demo', "iceberg.rest-catalog.warehouse=$n" | Set-Content -Path "catalog\$n.properties" -Encoding ascii
}
"connector.name=tpch" | Set-Content -Path catalog\tpch.properties -Encoding ascii

#Запуск движка trino
docker run -d --name trino -p 8080:8080 -v "${PWD}\catalog:/etc/trino/catalog" trinodb/trino:476
```
## 3. Разбор запроса — какие данные нужны
| Таблица TPC-H | Нужные колонки | Что запрос с ними делает |
|---|---|---|
| `part` | `partkey`, `brand`, `type`, `size` | соединение с `partsupp` по `partkey`, фильтрация, группировка и вывод `brand, type, size` |
| `partsupp` | `partkey`, `suppkey` | соединение с `part`; подсчёт `count(DISTINCT suppkey)` |
| `supplier` | `suppkey`, `comment` | отсеивает поставщиков, у которых `comment LIKE '%Customer%Complaints%'` |

Остальные колонки (`name`, `mfgr`, `container`, `retailprice`, `availqty`, `supplycost`, `address` и др.) в запросе не участвуют, поэтому в Silver мы их не переносим.
## 4. Медальон — решения по каждому слою
Как заполнять:
- **Строк** — `SELECT count(*) FROM таблица`;
- **Вес, МБ** — размер файлов таблицы в MinIO (консоль → бакет → объекты) или `SHOW STATS`;
- **Формат** — `PARQUET` / `ORC`;
- **Сжатие** — `NONE` / `SNAPPY` / `GZIP` / `ZSTD`;
- **Партиционирование** — по какой колонке (или «нет»);
- **Обоснование** — почему выбрал именно такой формат + сжатие + партиционирование для этого слоя и твоего объёма данных.
Источник данных — TPC-H sf5 из встроенного коннектора `tpch`. Каждый слой хранится как Iceberg-таблицы в схемах `bronze.q16`, `silver.q16`, `gold.q16`. Вес считали как сумму `file_size_in_bytes` по системным таблицам `"таблица$files"`.
### Bronze (сырой)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| `bronze.q16.part` | все | 1 000 000 | 20.2 | PARQUET | ZSTD | нет | В сыром слое данные лежат в том же виде, что и в источнике. Parquet хранит данные по колонкам, поэтому запрос читает только нужные. Таблицы небольшие, при партиционировании получилось бы много мелких файлов|
| `bronze.q16.partsupp` | все | 4 000 000 | 139.0 | PARQUET | ZSTD | нет | ⟨то же⟩ |
| `bronze.q16.supplier` | все | 50 000 | 2.5 | PARQUET | ZSTD | нет | ⟨то же⟩ |

Всего Bronze на ZSTD: 161.7 МБ.

### Silver (очищенный)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| `silver.q16.good_suppliers` | `suppkey` | 49 974 | 0.1 | PARQUET | ZSTD | нет | Поставщики без жалоб, таблица заменяет подзапрос `NOT IN`. |
| `silver.q16.part_supplier` | `brand`, `type`, `size`, `suppkey` | 592 670 | 2.0 | PARQUET | ZSTD | нет | Здесь уже применены фильтры запроса и соединение деталей с поставками, лишние колонки убраны, поэтому таблица занимает 2 МБ вместо 139 МБ у `partsupp`. Значения `brand`, `type`, `size` часто повторяются и хорошо сжимаются. Партиционирование по `size` ничего не отсекает, данные уже отфильтрованы до 8 значений.|

### Gold (витрина)

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| `gold.q16.supplier_counts` | `brand`, `type`, `size`, `supplier_cnt` | 27 724 | 0.1 | PARQUET | ZSTD | нет | Готовый результат запроса, весит мало; BI и отчёты читают его целиком.|
## 5. Тесты производительности
| Сжатие на Bronze | Время запроса (cold / warm) | Размер Bronze, МБ | Твой вывод (что лучше и почему) |
|---|---|---|---|
| GZIP | 4507 мс / 1931 мс | 146.7 | Экономит 15 МБ, запрос при этом выполняется дольше|
| ZSTD | 4038 мс / 1579 мс | 161.7 | Читается быстрее, размер почти такой же. Сырой слой часто перечитывается при сборке Silver и Gold, поэтому для него ZSTD выгоднее|


## 6. Часть 2 — осмысление и работа с инструментами
### 6.1. Понять и объяснить
| Вопрос | Твой ответ |
|---|---|
| Зачем нужны большие данные и lakehouse (а не обычная БД/хранилище)? | Обычную БД дорого и сложно масштабировать, и схему для неё нужно продумывать заранее.|
| Пример schema evolution на Silver | На копии `silver.q16.part_supplier_demo` мы выполнили `ALTER TABLE ... ADD COLUMN source varchar` и `ALTER TABLE ... RENAME COLUMN size TO part_size`. Файл данных остался один, как и до изменений: данные не переписывались, поменялась только схема в метаданных Iceberg. В старых строках новая колонка пустая. Переименованная колонка читается без проблем, так как Iceberg находит колонки по id. Новый снапшот при смене схемы не появляется|
| Пример скрипта восстановления из снапшота |Код ниже. До удаления было 592 670 строк, после удаления 568 149 (удалили 24 521 строку бренда `Brand#11`), после отката снова 592 670. |

Пример скрипта:
```sql

SELECT snapshot_id, committed_at, operation FROM silver.q16."part_supplier_demo$snapshots" ORDER BY committed_at;

DELETE FROM silver.q16.part_supplier_demo WHERE brand = 'Brand#11';

SELECT count(*) FROM silver.q16.part_supplier_demo FOR VERSION AS OF 4075415394085458874;

CALL silver.system.rollback_to_snapshot('q16', 'part_supplier_demo', 4075415394085458874);
``` 

### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбрал именно его |
|---|---|
| Apache Spark 3.5 | Работает с теми же Iceberg-таблицами в MinIO через тот же каталог Lakekeeper, данные копировать не нужно. Так видно ключевое свойство lakehouse: несколько движков работают с одними и теми же файлами. Настраивается просто и подходит для пакетной обработки и ETL, Trino при этом остаётся для интерактивных запросов|

Скрипты развёртывания:

```powershell
$ErrorActionPreference = "Stop"

#Создание необходимых папок
New-Item -ItemType Directory -Force -Path "spark/conf", "spark/jars" | Out-Null


$jars = @{
    "spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
    "spark/jars/iceberg-aws-bundle-1.7.1.jar" = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar"
}

foreach ($file in $jars.Keys) {
    if (!(Test-Path $file)) {
        curl.exe -fL --retry 3 -o $file $jars[$file]
    }
}

#Запуск Spark SQL
docker run -it --rm `
    -v "${PWD}\spark\conf:/opt/spark/conf" `
    -v "${PWD}\spark\jars:/opt/spark/iceberg-jars" `
    apache/spark:3.5.3 `
    /opt/spark/bin/spark-sql
```

Мои действия: ⟨`SHOW TABLES IN bronze.q16` — вывели список таблиц; `SELECT count(*) FROM bronze.q16.part` — получили 1 000 000; Q16 на Bronze — получили первые строки результата и `Time taken`⟩.

## 7. Выводы

В этой практической работе мы разобрали архитектуру Lakehouse и собрали медальонную структуру хранения: в Bronze лежат исходные данные, в Silver — подготовленные для анализа, в Gold — итоговая витрина. После обработки объём сократился со 161,7 МБ в Bronze до 2,0 МБ в Silver и 0,1 МБ в Gold, то есть ранняя фильтрация и агрегация заметно уменьшают данные. По замерам ZSTD выполнял запрос быстрее при размере Bronze всего на 15 МБ больше, чем у GZIP, поэтому для хранения мы выбрали его. Parquet со сжатием ZSTD хорошо подходит для колоночных данных и сокращает объём чтения в аналитических запросах. Подключение Apache Spark к тому же каталогу Lakekeeper показало, что разные движки работают с общими таблицами в объектном хранилище, и отдельные копии данных для этого не нужны.
