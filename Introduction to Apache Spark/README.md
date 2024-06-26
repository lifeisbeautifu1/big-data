# Introduction to Apache Spark

В ходе лабораторной работы был выполнен ряд заданий по анализу данных велопарковок Сан-Франциско (trips.csv, stations.csv). Анализ данных происходил посредством библиотеки `Spark` на нескольких языках программирования, таких как `python` и `Scala`. Сначала были загружены необходимые файлы с данными, затем они были предобработаны (отфильтрованы невалидные строки) и наконец преобразованы в необходимый формат.

Задания:

- Найти велосипед с максимальным временем пробега.
- Найти наибольшее геодезическое расстояние между станциями.
- Найти путь велосипеда с максимальным временем пробега через станции.
- Найти количество велосипедов в системе.
- Найти пользователей потративших на поездки более 3 часов.

## 1. Найти велосипед с максимальным временем пробега

Для выполнения данного задания исходные данные сперва были преобразованы в пару ключ значение, где ключем является уникальный идентификатор велосипеда, а значением является время пробега, затем была произведена группировка по ключу с последующей агрегацией по времени пробега. В результате мы получаем пары: (bike_id, total_duration). Остается только отсортировать наш список по времени пробега в нисходящем порядке и отобразить первый результат.

Python:

```python
bike_with_longest_duration = tripsInternal.keyBy(lambda trip: trip.bike_id) \
	.mapValues(lambda trip: trip.duration) \
	.reduceByKey(lambda firstDuration, secondDuration: firstDuration + secondDuration) \
	.sortBy(lambda trip: trip[1], ascending=False) \
	.first()
```

Scala:

```Scala
val bikeWithLongestDuration = tripsInternal.keyBy(trip => trip.bikeId)
	.mapValues(trip => trip.duration)
	.reduceByKey(_ + _)
	.sortBy(trip => trip._2, ascending = false)
	.first()
```

Результат:

> Bike id is 535 and maximum travel time is 18611693

![Result](https://i.imgur.com/etKwWzb.png)

## 2. Найти наибольшее геодезическое расстояние между станциями.

Для выполнения данного задания исходные данные нужно было преобразовать в множество всех возможных комбинаций станций друг с другом и их расстоянием. Затем эти данные были отсортированы в нисходящем порядке по расстоянию и был выведен первый результат.

Python:

```python
import math

def distance(a, b):
  #pi - число pi, rad - радиус сферы (Земли)
  rad = 6372

  # в радианах
  lat1  = a.lat  * math.pi / 180.
  lat2  = b.lat  * math.pi / 180.
  long1 = a.long * math.pi / 180.
  long2 = b.long * math.pi / 180.

  # косинусы и синусы широт и разницы долгот
  cl1 = math.cos(lat1)
  cl2 = math.cos(lat2)
  sl1 = math.sin(lat1)
  sl2 = math.sin(lat2)
  delta = long2 - long1
  cdelta = math.cos(delta)
  sdelta = math.sin(delta)

  # вычисления длины большого круга
  y = math.sqrt(math.pow(cl2 * sdelta, 2) + math.pow(cl1 * sl2 - sl1 * cl2 * cdelta,2))
  x = sl1 * sl2 + cl1 * cl2 * cdelta
  ad = math.atan2(y, x)
  dist = ad * rad
  return dist

result = stationsInternal.cartesian(stationsInternal) \
		.map(lambda pair: (pair[0].name, pair[1].name, distance(pair[0], pair[1]))) \
		.sortBy(lambda station: station[2], ascending=False) \
		.first()

print(f"From station {result[0]} to station {result[1]} the distance is: {result[2]} in kilometers")
```

Scala:

```Scala
def distance( a: Station, b: Station ) : Double = {
	val rad = 6372
	val lat1   = a.lat  * math.Pi / 180
	val lat2   = b.lat  * math.Pi / 180
	val long1  = a.long * math.Pi / 180
	val long2  = b.long * math.Pi / 180

	val cl1 = math.cos(lat1)
	val cl2 = math.cos(lat2)
	val sl1 = math.sin(lat1)
	val sl2 = math.sin(lat2)
	val delta = long2 - long1
	val cdelta = math.cos(delta)
	val sdelta = math.sin(delta)

	val y = math.sqrt(math.pow(cl2 * sdelta, 2) + math.pow(cl1 * sl2 - sl1 * cl2 * cdelta, 2))
	val x = sl1 * sl2 + cl1 * cl2 * cdelta
	val ad = math.atan2(y, x)
	val dist = ad * rad
	return dist
}

val longestDistance = stationsInternal.cartesian(stationsInternal)
	.map(pair => (pair._1.name, pair._2.name, distance(pair._1, pair_2)))
	.sortBy(list => list._3, ascending = false)
	.first()
```

Результат:

> From station SJSU - San Salvador at 9th to station Embarcadero at Sansome the distance is: 69.9318508210141 in kilometers

![Result](https://i.imgur.com/vT7wvks.png)

## 3. Найти путь велосипеда с максимальным временем пробега через станции.

Для выполнения данного задания необходимо аналогично первому заданию найти уникальный идентификатор велосипеда с максимальным пробегом через станции и затем вывести последовательно названия станций через которые держал путь наш велосипед.

Python:

```python
paths = tripsInternal.filter(lambda trip: trip.bike_id == bike_with_longest_duration[0]) \
  .sortBy(lambda trip: trip.start_date) \
  .take(10)
```

Scala:

```Scala
val paths = tripsInternal.filter(trip => trip.bikeId == bikeWithLongestDuration._1)
	.sortBy(trip => trip.startDate)
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

![Result](https://imgur.com/dsbfNA9.png)

## 4. Найти количество велосипедов в системе.

Для выполнения данного задания нужно просто узнать количество уникальных идентификаторов `bike_id`.

Python:

```python
bikes_count = tripsInternal.map(lambda trip: trip.bike_id) \
	.distinct() \
	.count()
```

Scala:

```Scala
val bikesCount = tripsInternal.map(trip => trip.bikeId)
	.distinct()
	.count()
```

Результат:

> 700

![Result](https://imgur.com/QbrEcnG.png)

## 5. Найти пользователей потративших на поездки более 3 часов.

Так как в наших исходных данных нет как такового уникального идентификатора пользователя, я решил задание используя почтовый индекс, хотя у разных пользователей почтовый индекс может повторяться.

Python:

```python
subscribers = tripsInternal.keyBy(lambda trip: trip.zip_code) \
  .mapValues(lambda trip: trip.duration) \
  .reduceByKey(lambda firstDuration, secondDuration: firstDuration + secondDuration) \
  .filter(lambda trip: trip[1] > 3 * 60 * 60 and trip[0] != 'nil' and trip[0] != "") \
  .take(10)
```

Scala:

```Scala
val subscribers = tripsInternal.keyBy(trip => trip.zipCode)
	.mapValues(trip => trip.duration)
	.reduceByKey(_ + _)
	.filter(trip => trip._2 > 3 * 60 * 60)
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

![Result](https://imgur.com/q4aQ1gn.png)
