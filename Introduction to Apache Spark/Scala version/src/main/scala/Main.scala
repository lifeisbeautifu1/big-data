import org.apache.spark._
import java.time._
import java.time.format.DateTimeFormatter
import scala.math._

case class Station(
										stationId: Integer,
										name: String,
										lat: Double,
										long: Double,
										dockcount: Integer,
										landmark: String,
										installation: String)

case class Trip(
								 tripId: Integer,
								 duration: Integer,
								 startDate: LocalDateTime,
								 startStation: String,
								 startTerminal: Integer,
								 endDate: LocalDateTime,
								 endStation: String,
								 endTerminal: Integer,
								 bikeId: Integer,
								 subscriptionType: String,
								 zipCode: String)

object Main {
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

  def main(args: Array[String]): Unit = {
		val config = new SparkConf()
			.setAppName("Introduction to Apache Spark")
			.setMaster("local[*]")

		val sparkContext = new SparkContext(config)

		val tripData = sparkContext.textFile("data/trips.csv")
		// запомним заголовок, чтобы затем его исключить
		val tripsHeader = tripData.first
		val trips = tripData.filter(row => row!= tripsHeader).map(row => row.split(",", -1))

		val stationData = sparkContext.textFile("data/stations.csv")
		val stationsHeader = stationData.first
		val stations = stationData.filter(row => row != stationsHeader).map(row => row.split(",", -1))

		val tripsInternal = trips.mapPartitions(rows => {
			val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
			rows.map( row =>
				new Trip(tripId=row(0).toInt,
					duration=row(1).toInt,
					startDate=LocalDateTime.parse(row(2), timeFormat),
					startStation=row(3),
					startTerminal=row(4).toInt,
					endDate=LocalDateTime.parse(row(5), timeFormat),
					endStation=row(6),
					endTerminal=row(7).toInt,
					bikeId=row(8).toInt,
					subscriptionType=row(9),
					zipCode=row(10)))})

		val stationsInternal = stations.map(row=>
			new Station(stationId=row(0).toInt,
				name=row(1),
				lat=row(2).toDouble,
				long=row(3).toDouble,
				dockcount=row(4).toInt,
				landmark=row(5),
				installation=row(6)))

		val bikeWithLongestDuration = tripsInternal.keyBy(trip => trip.bikeId)
			.mapValues(trip => trip.duration)
			.reduceByKey(_ + _)
			.sortBy(trip => trip._2, ascending = false)
			.first()

		println("Bike id is " + bikeWithLongestDuration._1 + " and maximum duration is " + bikeWithLongestDuration._2)

		val longestDistance = stationsInternal.cartesian(stationsInternal)
			.map(pair => (pair._1.name, pair._2.name, distance(pair._1, pair._2)))
			.sortBy(list => list._3, ascending = false)
			.first()

		println(longestDistance)

		val paths = tripsInternal.filter(trip => trip.bikeId == bikeWithLongestDuration._1)
			.sortBy(trip => trip.startDate)
			.take(10)

		paths.foreach {
			path => println("From station " + path.startStation + " to station: " + path.endStation)
		}

		val bikesCount = tripsInternal.map(trip => trip.bikeId)
			.distinct()
			.count()

		println(bikesCount)

		val subscribers = tripsInternal.keyBy(trip => trip.zipCode)
			.mapValues(trip => trip.duration)
			.reduceByKey(_ + _)
			.filter(trip => trip._2 > 3 * 60 * 60)
			.take(10)

		subscribers.foreach(println)

		sparkContext.stop()
  }
}
