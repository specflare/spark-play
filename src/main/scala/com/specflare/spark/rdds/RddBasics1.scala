package com.specflare.spark.rdds

import com.specflare.spark.InitSpark

// Tags: mapValues, reduceByKey
object RddBasics1 extends App with InitSpark {
  // Data format: ID, Name, age, numFriends
  val data = Seq((1, "Liviu", 34, 45)
    , (2, "Ion", 12, 25)
    , (3, "Andreea", 31, 67)
    , (4, "Razvan", 34, 2)
    , (5, "Andra", 31, 15)
  )

  val rdd = spark.sparkContext.parallelize(data)
  rdd.foreach(println)
  rdd.foreach(entry => println(s"Name: ${entry._2}, Age: ${entry._3}"))

  // compute the average number of friends per age
  val results = rdd.map(entry => (entry._3, entry._4))  // keep only age and numFriends
    .mapValues(v => (v, 1))
    .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)) // now we have number of friends and the number of additions.
    .mapValues(v => (v._1 / v._2))
    .collect()

  results.sorted.foreach(println)
  close()
}
