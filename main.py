"""1. Работа с аргументами функции
Задание: Напиши функцию process_data, которая принимает неограниченное количество позиционных
и именованных аргументов.

Функция должна складывать все переданные числовые значения.
Все именованные аргументы должны выводиться как ключ-значение."""

# def process_date(*args,**kwargs):
#     # Сумма числовых значений args
#     sum_of_args = sum(arg for arg in args if isinstance(arg,(int,float)))
#     print(f"Sum of positional arguments: {sum_of_args}")
#     # Вывод именованных аргументов
#     for key, value in kwargs.items():
#         print(f"{key}:{value}")
# process_date(3,4,3,4,a='boris',main='trikko')
#


# '''Тренировочный код '''
# def greet(name, age):
#     print(f"Hello, {name}! You are {age} years old.")
#
# greet('Alice', 23) #позиционные аргмуенты: для name - 'Alice', для age - 23
# greet(age='Alice', name=23) #именованные - соблюдаем порядок
# #в Python через *args, **kwargs мы принимаем любое количество позиционных аргументов.
# def process_data(*args, **kwargs):
#     print("Positional arguments:", args)
#     print("Named arguments:", kwargs)
#
# process_data(10,20,30,name='Dataset A', date='2024-10-03')

def square(x):
    print(x * x)

# Использование функции в выражении
result = square(5) + 10
print(result)  # Выведет: 35
