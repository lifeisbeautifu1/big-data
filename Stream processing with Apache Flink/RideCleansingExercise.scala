/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_scala.basics

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * The "Ride Cleansing" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed to the
 * standard out.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object RideCleansingExercise extends ExerciseBase {
  def main(args: Array[String]) {
    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToRideData)

    val maxDelay = 60 // events are out of order by max 60 seconds
    val speed = 600   // events of 10 minutes are served in 1 second

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // get the taxi ride data stream
    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    val filteredRides = rides
      // filter out rides that do not start and end in NYC
      .filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))

    // print the filtered stream
    printOrTest(filteredRides)

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }
}
