package com.specflare.spark.rdds

import com.specflare.spark.InitSpark

object RddMovieLens extends App with InitSpark {
  val movieNames =
    sc.textFile("src/main/resources/ml-100k/u.item")
    .map(row => (row.split("\\|")(0).toInt, row.split("\\|")(1)))
    .collectAsMap()
    // .foreach(println)

  var nameDict = sc.broadcast(movieNames) // broadcast nameDict to all workers

  // Data format: UserId, MovieId, Rating, Timestamp
  val ratings = sc.textFile("src/main/resources/ml-100k/ub.test")

  // find the most popular movie (rated by the most users)
  ratings.map(rating => (rating.split("\t")(1).toInt, 1))
    .reduceByKey((x, y) => x + y) // for each movie we have the number of ratings
    .map(x => (x._2, x._1)) // flip order
    .sortByKey()
    .foreach(row => println(s"Movie '${nameDict.value(row._2)}' was rated ${row._1} times."))
}
