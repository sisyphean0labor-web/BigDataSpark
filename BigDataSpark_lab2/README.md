# Больше для себя, чем для читающего (0_0)
--остановить докер и очистить данные
cd ~/Projects/Anal_BD/bds_lab2/BigDataSpark_lab2
sudo docker-compose down -v
sudo rm -rf pg_data
mkdir pg_data

--запустить контейнер
sudo docker-compose up -d

--Проверка строк
sudo docker exec -it bds_postgres psql -U admin -d bds_lab -c "SELECT COUNT(*) FROM mock_data_raw;"

--проверка сырости
sudo docker exec -it bds_postgres psql -U admin -d bds_lab -c "
SELECT 'dim_customer' AS name, COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"

#первые пять строк самой "проблемной" таблицы
SELECT * FROM fact_sales LIMIT 5;

--база в клик хаус
docker exec -it bds_clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS reports;"

--получить ссылку на Юпитер
sudo docker logs bds_jupyter 2>&1 | grep "http://127.0.0.1:8888/lab?token="

--дальше вставляем код из питон файла в яче ку юпитера по полученной ссылке
sudo docker exec -it bds_clickhouse clickhouse-client

--вход в клик хаус
sudo docker exec -it bds_clickhouse clickhouse-client
_________________________________________________________________________________________


# BigDataSnowflake
Анализ больших данных - лабораторная работа №1 - нормализация данных в снежинку

Одна из задач data engineer при работе с данными BigData трансформировать исходную модель данных источника в аналитическую модель данных. Аналитическая модель данных позволяет исследовать данные и принимать на основе полученных данных решения. Классическими универсальными схемами для анализа данных являются "звезда" и "снежинка". В лабораторной работе вам предстоит потренироваться в трансформации исходных данных из источников в модель данных снежинка.

Что необходимо сделать?

Необходимо данные источника (файлы mock_data.csv с номерами), которые представляют информацию о покупателях, продавцах, поставщиках, магазинах, товарах для домашних питомцев трансформировать в модель снежинка/звезда (факты и измерения с нормализацией).

<img width="1411" height="692" alt="Лабораторная работа 1" src="https://github.com/user-attachments/assets/0282c756-76a3-48f7-86e4-df6e1ec6ac89" />


Алгоритм:
1. форкнуть к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Запускаете базу данных PostgreSQL (рекомендую установку через docker).
4. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
5. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
6. Анализируете исходные данные с помощью запросов.
7. Выявляете сущности фактов и измерений.
8. Реализуете скрипты DDL для создания таблиц фактов и измерений.
9. Реализуете скрипты DML для заполнения таблиц фактов и измерений из исходных данных.
10. Проверяете полученный результат.
11. Отправляете результат на проверку лаборантам.
12. Обсуждаете работу с лаборантами.

Что должно быть результатом работы?
1. Репозиторий, в котором есть исходные данные mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL и заполненными данными из файлов mock_data(*).csv.
3. Скрипты DDL (SQL) создания таблиц фактов и измерений в соответствии с моделью снежинка/звезда.
4. Скрипты DML (SQL) заполнения таблиц фактов и измерений из исходных данных.
