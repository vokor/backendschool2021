[![Build Status](https://travis-ci.com/vokor/backendschool2021.svg?token=qCnqbxaGF2UTqcxu1s1x&branch=main)](https://travis-ci.com/github/vokor/backendschool2021)
##  Описание задания
Чтобы немного скрасить жизнь людей на самоизоляции, вы решаете открыть
интернет-магазин по доставке конфет "Сласти от всех напастей".

Ваша задача — разработать на python REST API сервис, который позволит нанимать курьеров на работу,
принимать заказы и оптимально распределять заказы между курьерами, попутно считая их рейтинг и заработок.


## Реализованные обработчики REST API
   * POST /couriers
   * PATCH /couriers/$courier_id
   * POST /orders
   * POST /orders/assign
   * POST /orders/complete

## Запуск приложения

   * Docker Compose

Находясь в папке с файлом `docker-compose.yml` выполнить в терминале:

    docker-compose build
    docker-compose up

   * Вручную 

    pip install -r requirements.txt
    python index.py

   * Запуск тестов

	pip install -r requirements.txt
	python -m unittest discover -s tests/ -p '*_tests.py'
