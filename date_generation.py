import pandas as pd
from faker import Faker
import random
from sqlalchemy import create_engine
import clickhouse_connect
import time

# Запускаем таймер
start_time = time.time()

fake = Faker()
num_rows = 1000

# Генерация данных для клиентов
customers = {
    "customer_id" : [i for i in range(1, num_rows + 1)],
    "age" : [random.randint(18, 70) for _ in range(num_rows)],
    "gender" : [random.choice(["Male", "Female"]) for _ in range(num_rows)],
    "city" : [fake.city() for _ in range(num_rows)],
    "registration_date" : [fake.date_this_decade() for _ in range(num_rows)]
}

customer_data = pd.DataFrame(customers)
customer_data.to_csv('customer_data.csv', index=False)

# Генерация данных для товаров
products = {
    "product_id": [i for i in range(1, num_rows + 1)],
    "category": [random.choice(["Electronics", "Clothing", "Furniture", "Toys", "Books"]) for _ in range(num_rows)],
    "product_name": [fake.catch_phrase() for _ in range(num_rows)],
    "supplier": [fake.company() for _ in range(num_rows)],
    "price": [round(random.uniform(5, 500), 2) for _ in range(num_rows)]
}

product_data = pd.DataFrame(products)
product_data.to_csv("product_data.csv", index=False)

# Генерация данных для продаж
sales = {
    "sale_id": [i for i in range(1, num_rows + 1)],
    "sale_date": [fake.date_this_year() for _ in range(num_rows)],
    "product_id": [random.randint(1, num_rows) for _ in range(num_rows)],
    "quantity": [random.randint(1, 10) for _ in range(num_rows)],
    "price": [round(random.uniform(10, 500), 2) for _ in range(num_rows)],
    "customer_id": [random.randint(1, num_rows) for _ in range(num_rows)]
}

sales_data = pd.DataFrame(sales)
sales_data.to_csv("sales_data.csv", index=False)


# Подключение к базе данных
engine = create_engine('postgresql://postgres:rootroot@localhost:5432/new_db')

# Чтение СSC и загрузка данных для product_data и sales_data

product_data = pd.read_csv('product_data.csv')
product_data.to_sql('product_data', engine, if_exists='replace', index=False)

sales_data = pd.read_csv('sales_data.csv')
sales_data.to_sql('sales_data', engine, if_exists='replace',index=False)

# Извлечение данных о продажах из PostgreSQL для агригирующих операций в будущем
sales_data = pd.read_sql_query('SELECT sale_date, product_id, quantity, price FROM sales_data',engine)
#print(sales_data.head())
# Агрегирование данных: общая сумма продаж и количество
summary_sales = sales_data.groupby(['sale_date', 'product_id']).agg(
    total_quantity=('quantity', 'sum'),
    total_sales=('price', 'sum')
).reset_index()
#print(summary_sales)

# Подключаемся к Clickhouse
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
result = client.query('SELECT version()')
#print(result.result_rows)  # Должен вывести версию ClickHouse

# Создаем таблицу для агрегированных данных о продажах

client.command('''
    CREATE TABLE IF NOT EXISTS summary_sales (
        sale_date Date,
        product_id UInt32,
        total_quantity UInt32,
        total_sales Float32
    ) ENGINE = MergeTree()
    ORDER BY sale_date
''')

# 1. Cпособ Загрузка каждой агрегированной строки данных в Clickhouse через цикл
# for _, row in summary_sales.iterrows(): # тут_, означает что мы пропускаем индекс вызванный методом itterows(), двигаясь по строкам
#     client.command('''
#         INSERT INTO summary_sales (sale_date, product_id, total_quantity, total_sales) VALUES
#         (%s, %s, %s, %s)
#     ''', (row['sale_date'], row['product_id'], row['total_quantity'], row['total_sales']))
# Как %s предотвращает SQL-инъекции?
# Когда используются placeholders вроде %s, они отделяют значения от структуры запроса. Вместо того чтобы подставлять данные напрямую в запрос,
# мы передаём их как параметры, и библиотека автоматически подставляет их безопасным способом.
# Библиотека clickhouse-connect (и другие аналогичные) берёт значения из row['sale_date'],
# row['product_id'] и т. д., экранирует их и правильно добавляет в запрос.

# 2. Cпособ батчевой загрузки через CSV. Убирая индексы и заголовки
summary_sales.to_csv('summary_sales.csv', index=False, header=False)

# Откроем CSV и загрузим все данные одним запросом
with open('summary_sales.csv', 'r') as f:
    csv_data = f.read()  # Чтение данных из файла как одной строки
    client.command("INSERT INTO summary_sales FORMAT CSV " + csv_data)


# Загружаем все данные одним запросом
# client.insert('summary_sales', data_to_insert)

# Проверочный запрос в Clickhouse
result = client.query('SELECT * from summary_sales LIMIT 10')
print(result.result_rows) # Печать первых 10 строк для проверки
# Остановка таймера
end_time = time.time()

# Вычисляем время исполнения
execution_time = end_time - start_time
print('Время выполнения кода: {:2f} секунд'.format(execution_time))

# Подсчет общего количества строк в таблице
result = client.query('SELECT COUNT(*) FROM summary_sales')
print(f"Общее количество строк:{result.result_rows[0][0]}")
'''Такой синтаксис нужен, потому что clickhouse-connect возвращает результаты в виде двумерного массива 
(списка списков), что позволяет обрабатывать многократные строки и столбцы, 
как если бы это был результат с несколькими строками и колонками.'''

# Подсчет суммарных продаж и количества товаров по датам.
result = client.query('''
    SELECT sale_date, SUM(total_sales) AS total_sales, SUM(total_quantity) AS total_quantity
    FROM summary_sales
    GROUP BY sale_date
    ORDER BY sale_date
''')
print('Суммарные продажи и количество по датам:',result.result_rows[:5])

"""Шаги для оптимизации и анализа"""
"""Шаг 1: Оптимизация таблицы с помощью партиционирования
Мы можем создать новую версию таблицы summary_sales_partitioned, 
добавив в неё партиционирование по sale_date, 
чтобы ClickHouse мог быстрее обрабатывать запросы. Пример команды для создания:"""

client.command('''
    CREATE TABLE IF NOT EXISTS summary_sales_partitioned (
        sale_date Date,
        product_id UInt32,
        total_quantity UInt32,
        total_sales Float32
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(sale_date) -- Партиционирование по месяцу
    ORDER BY (sale_date, product_id)
''')

# Вставляем данные из исходной таблицы
client.command('INSERT INTO summary_sales_partitioned SELECT * FROM summary_sales')