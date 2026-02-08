### english version is below
## русскоязычная версия:
# simple_gp_airflow_clickhouse_superset_project
Реализация ETL-процесса в Greenplum в сочетании с простой оркестрацией через Apache Airflow, ручной загрузкой витрины данных в ClickHouse и подготовкой датасета и дэшборда в Apache Superset.
Используемая программная среда:
Локально: DBeaver 25.3.1; gpfdist из дистрибутива greenplum-db-clients-6.20.0
Удалённые экземпляры БД: Greenplum Database 6.22.1; ClickHouse 22.8.6.71

В рамках данного проекта были реализованы следующие шаги:
1. Создание Append-Optimized (AO) таблиц фактов с распределением и партиционированием, а также создание справочных Heap таблиц;
2. Создание внешних readable таблиц с использованием протокола PXF (для исходных таблиц фактов из сторонней БД PostgreSQL) и протокола gpfdist (для исходных справочных таблиц на локальном устройстве);
3. Реализация пользовательских функций для ETL с использованием метода Delta Partition для таблиц фактов и метода полной загрузки (Full Load) для справочных таблиц;
4. Реализация пользовательских функций для создания требуемой витрины данных;
5. Анализ коэффициента перекоса для AO-таблиц с целью корректного выбора ключа распределения;
6. Реализация простого DAG на Python для оркестрации загрузки данных с помощью Apache Airflow;
7. Ручная загрузка созданной витрины из Greenplum в кластер ClickHouse, включая создание интеграционных таблиц, словарей, а также реплицируемых и распределённых таблиц;
8. Подключение Apache Superset, создание датасетов на основе витрины и словарей ClickHouse, а также разработка графиков и дашборда.

## english version:
# simple_gp_airflow_clickhouse_superset_project
Implementation of GP ETL combined with simple Apache Airflow orchestration, manual ClickHouse mart load and Apache Superset dataset and dashboard preparation.
Software environment:
Local: DBeaver 25.3.1; gpfdist from greenplum-db-clients-6.20.0 distribution kit
Remote DB instances: Greenplum Database 6.22.1; ClickHouse 22.8.6.71;

During this simple project following steps were implemented:
1. Appendoptimized (AO) fact tables with distribution and partitioning and heap dimension tables creation;
2. External readable tables creation with PXF protocol (for PostgreSQL source fact tables) and gpfdist protocol (for local source dimension tables);
3. User defined function implementation for ETL by using Delta Partition method for Fact tables and Full Load method for Dimension tables;
4. User defined function implementation for required mart creation;
5. Analysis of skew coefficient for AO tables for correct choice of Distribution key;
6. Simple DAG Python implementation for data load orchestration by Apache Airflow;
7. Manual load of created mart from GP into ClickHouse cluster including creation of integration tables and dictionaries, replicated and distributed tables;
8. Connection of Apache Superset, dataset creation based on ClickHouse mart and dictionaries, charts and dashboard creation.
