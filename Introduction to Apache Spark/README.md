# Introduction to Apache Spark

В ходе лабораторной работы был выполнен ряд заданий по анализу данных велопарковок Сан-Франциско (trips.csv, stations.csv). Анализ данных происходил посредством библиотеки `pyspark`. Сначала были загружены необходимые файлы с данными, затем они были предобработаны (отфильтрованы невалидные строки) и наконец преобразованы в необходимый формат.

Задания:

- Найти велосипед с максимальным временем пробега.
- Найти наибольшее геодезическое расстояние между станциями.
- Найти путь велосипеда с максимальным временем пробега через станции.
- Найти количество велосипедов в системе.
- Найти пользователей потративших на поездки более 3 часов.

## 1. Найти велосипед с максимальным временем пробега

Для выполнения данного задания исходные данные сперва были преобразованы в пару ключ значение, где ключем является уникальный идентификатор велосипеда, а значением является время пробега, затем была произведена группировка по ключу с последующей агрегацией по времени пробега. В результате мы получаем пары: (bike_id, total_duration). Остается только отсортировать наш список по времени пробега в нисходящем порядке и отобразить первый результат.

```python
bike_with_longest_duration = tripsInternal.keyBy(lambda trip: trip.bike_id) \
.mapValues(lambda trip: trip.duration) \
.reduceByKey(lambda firstDuration, secondDuration: firstDuration + secondDuration) \
.sortBy(lambda trip: trip[1], ascending=False) \
.first()
```

Результат:

> Bike id is 535 and maximum travel time is 18611693

## 2. Найти наибольшее геодезическое расстояние между станциями.

Для выполнения данного задания исходные данные нужно было преобразовать в множество всех возможных комбинаций станций друг с другом и их расстоянием. Затем эти данные были отсортированы в нисходящем порядке по расстоянию и был выведен первый результат.

```python
result = stationsInternal.cartesian(stationsInternal) \
.map(lambda pair: (pair[0].name, pair[1].name, (pair[0].lat ** 2 - pair[1].lat ** 2) + (pair[0].long ** 2 - pair[1].long ** 2) ** 0.5)) \
.map(lambda station: (station[0], station[1], station[2].real if isinstance(station[2], complex) else station[2])) \
.sortBy(lambda station: station[2], ascending=False) \
.first()
```

Результат:

> From station Embarcadero at Sansome to station San Salvador at 1st the distance is: 46.90200872492147

## 3. Найти путь велосипеда с максимальным временем пробега через станции.

Для выполнения данного задания необходимо аналогично первому заданию найти уникальный идентификатор велосипеда с максимальным пробегом через станции и затем вывести последовательно названия станций через которые держал путь наш велосипед.

```python
paths = tripsInternal.filter(lambda trip: trip.bike_id == bike_with_longest_duration[0]) \
  .sortBy(lambda trip: trip.start_date) \
  .take(10)
```

Результат:

> From station: Post at Kearney to station: San Francisco Caltrain (Townsend at 4th)
> From station: San Francisco Caltrain (Townsend at 4th) to station: San Francisco Caltrain 2 (330 Townsend)
> From station: San Francisco Caltrain 2 (330 Townsend) to station: Market at Sansome
> From station: Market at Sansome to station: 2nd at South Park
> From station: 2nd at Townsend to station: Davis at Jackson
> From station: San Francisco City Hall to station: Civic Center BART (7th at Market)
> From station: Civic Center BART (7th at Market) to station: Post at Kearney
> From station: Post at Kearney to station: Embarcadero at Sansome
> From station: Embarcadero at Sansome to station: Washington at Kearney
> From station: Washington at Kearney to station: Market at Sansome

## 4. Найти количество велосипедов в системе.

Для выполнения данного задания нужно просто узнать количество уникальных идентификаторов `bike_id`.

```python
bikes_count = tripsInternal.map(lambda trip: trip.bike_id) \
.distinct() \
.count()
```

Результат:

> 700

## 5. Найти пользователей потративших на поездки более 3 часов.

Так как в наших исходных данных нет как такового уникального идентификатора пользователя, я решил задание используя почтовый индекс, хотя у разных пользователей почтовый индекс может повторяться.

```python
subscribers = tripsInternal.keyBy(lambda trip: trip.zip_code) \
  .mapValues(lambda trip: trip.duration) \
  .reduceByKey(lambda firstDuration, secondDuration: firstDuration + secondDuration) \
  .filter(lambda trip: trip[1] > 3 * 60 * 60 and trip[0] != 'nil' and trip[0] != "") \
  .take(10)
```

Результат:

> ('95060', 758576)
> ('94109', 12057128)
> ('94061', 3049397)
> ('94612', 1860796)
> ('95138', 155295)
> ('94123', 1895963)
> ('94133', 21637675)
> ('94960', 1439873)
> ('94131', 3143302)
> ('1719', 24561)
