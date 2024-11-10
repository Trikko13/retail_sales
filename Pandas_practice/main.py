import pandas as pd

"""1. Загрузка файла в DataFrame"""

# df = pd.read_csv('master.csv') # файл в папке
# print(df) # вывести первые 5 и последние 5 строк
# print(df.head()) # вывести первые 5 строк
# print(df.info()) # общая инфа о данных
# print(df.describe()) # базовые статистические хар-ки

"""2. Фильтрация данных
Задача: Отфильтруй данные, чтобы увидеть записи 
только для определённой страны и определённого года, например, для Japan в 2015."""

# Фильтрация по стране и году
# filtered_df = df[(df['country'] == 'Japan') & (df['year'] == 2015)]
# print(filtered_df)

"""3. Группировка и агрегация
Задача: Посчитай общее количество самоубийств и 
средний ВВП на душу населения для каждой страны."""

# Группировка по возрасту и полу

# grouped_df = df.groupby(['age','sex']).agg({
#     'suicides_no': 'sum',
#     'gdp_per_capital ($)': 'mean'
# }).reset_index()
# print(grouped_df.head())

# Годовая статистика по странам:
#
# yearly_country_df = df.groupby(['year','country']).agg({
#     'suicides/100k pop': 'mean',
# }).reset_index()
# print(yearly_country_df.head())
#
"""4. Создание DataFrame"""
# из словаря
# data = {'country': ['japan','France', 'USA'],
#         'population' : [134,134,23]}
# df = pd.DataFrame(data)
# print(df)
# из списка списков (матрицы)
# data = [['Japan', 34], ['France', 53],['USA', 342]]
# df = pd.DataFrame(data, columns=['country', 'population'])
# print(df)
"""4.1 Создание нового столбца в DataFrame"""
# добавление нового столбца в DF с фиксированным значением
# df['continent'] = 'Asia'
# print(df)
# добавление с вычислением из текущих столбцов
# my_df = pd.read_csv('master.csv')
# my_df['suicides_per_100k'] = (my_df['suicides_no'] / my_df['population']) * 100000
# print(my_df.info())

"""5. Создание Series """
# из списка
# population = pd.Series([331, 34, 13], name='population')
# print(population)

# из словаря
# population = pd.Series({'Japan': 32, 'France': 334, 'USA': 341})
# print(population)

"""6. Типы данных и базовые операции."""
# посмотреть текущие типы данных у ДатаФрейма
# print(my_df.dtypes)

# преобразования столбца population к типу float
# my_df['population'] = my_df['population'].astype(int)

# Преобразование в формат даты (но столбца с датами нет).
# my_df['date'] = pd.to_datetime(my_df['date'])
# print(my_df['age'])

"""7. Базовые операции с DataFrame"""
# сумма, среднее, минимум, максимум
# print(my_df['population'].sum())
# print(my_df['population'].mean())
# print(my_df['population'].min())
# print(my_df['population'].max())

# Фильтрация по значению
# high_population_df = my_df[my_df['population'] > 100]
# print(high_population_df)

# Группировка и агрегация
# grouped_1 = my_df.groupby(['age']).min()
# grouped_2 = my_df.groupby(['age']).max()
# print(grouped_1, grouped_2)

'''Эти команды покрывают основные операции в Pandas, 
которые помогут уверенно работать с данными и создавать отчёты.'''
import pandas as pd

# Загрузка данных из двух CSV-файлов
df = pd.read_csv("drift_perp.csv")  # первый датасет
new_data = pd.read_csv("drift_perp_2.csv")  # второй датасет

# Преобразование колонки 'datetime' в тип datetime
df['datetime'] = pd.to_datetime(df['datetime'])
new_data['datetime'] = pd.to_datetime(new_data['datetime'])

# Объединение двух датасетов
combined_df = pd.concat([df, new_data], ignore_index=True)
print(combined_df.info())

# Фильтрация по дате от 2 октября 2024 года
filtered_df = combined_df[combined_df['datetime'] == '2024-10-25']

# Группировка данных по колонке 'market' и суммирование 'pnl'
grouped_tokens = filtered_df.groupby('market', as_index=False)['pnl'].sum()

# Итоговая сумма всех 'pnl' по всем рынкам
total_tokens_pnl = grouped_tokens['pnl'].sum()

# Добавление новой строки с итоговой суммой в DataFrame
total_row = pd.DataFrame({'market': ['Total'], 'pnl': [total_tokens_pnl]})
grouped_tokens = pd.concat([grouped_tokens, total_row], ignore_index=True)

print(grouped_tokens)





















































































































































































































