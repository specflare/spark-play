package com.specflare.spark.rdds

import com.specflare.spark.InitSpark

case class Person (firstName: String, lastName: String, country: String, age: Int)

object RddFromCSV extends App with InitSpark {
  val rdd = sc.textFile("src/main/resources/people-country-age.csv") // CSV contains a header
  rdd.foreach(println)
  val header = rdd.first()
  val rdd_without_header = rdd.filter(row => row != header)
  println("RDD without header:")
  rdd_without_header.foreach(println)
}
