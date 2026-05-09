# Больше для себя, чем для читающего (0_0)
--остановить докер и очистить данные
cd ~/Projects/Anal_BD/bds_lab2/BigDataSpark_lab2
sudo docker-compose down -v
sudo rm -rf pg_data
mkdir pg_data

--запустить контейнер
sudo docker-compose up -d


--получить ссылку на Юпитер
sudo docker logs bds_jupyter 2>&1 | grep "http://127.0.0.1:8888/lab?token="
--дальше вставляем код из питон файла в яче ку юпитера по полученной ссылке

--максимальная проверка
sudo docker exec -it bds_postgres psql -U admin -d bds_lab -c "\dt"
sudo docker exec -it bds_clickhouse clickhouse-client --query "SELECT * FROM reports.top_10_products"


--вход в клик хаус
sudo docker exec -it bds_clickhouse clickhouse-client


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

_________________________________________________________________________________________
