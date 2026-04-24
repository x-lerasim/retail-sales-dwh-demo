# retail-sales-dwh-demo

Демо-проект для генерации B2B продаж и загрузки данных в `PostgreSQL` через контейнер `generator`.

## Запуск генератора

Историческая загрузка:

`docker compose run --rm generator --mode historical --orders 50000 --days 90`

Инкрементальная загрузка:

`docker compose run --rm generator --mode incremental`

## Что генерируется

- справочники клиентов и товаров
- заказы и позиции
- отгрузки
- история статусов
- оплаты
- возвраты
