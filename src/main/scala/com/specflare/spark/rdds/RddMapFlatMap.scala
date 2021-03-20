package com.specflare.spark.rdds

import com.specflare.spark.InitSpark

// Tags: map, flatMap
object RddMapFlatMap extends App with InitSpark {
  val lines = sc.textFile("src/main/resources/multiline-sentence.txt")
  lines.map(line => line.toUpperCase()).foreach(println)

  // convert each elem of the RDD (each line) to a list of words
  println("flatMap: return list of words in the sentences, and then count each word:")
  lines.flatMap(line => line.split("\\W+"))
    .map(word => word.toLowerCase())
    .countByValue()
    .foreach(println)

  // print word occurrences sorted
  println("Print sorted word occurrences:")
  lines.flatMap(line => line.split("\\W+"))
    .map(word => word.toLowerCase())
    .map(word => (word, 1))
    .reduceByKey((x, y) => x + y)
    .map(x => (x._2, x._1)) // flip pair so we can sortByKey
    .sortByKey()
    .foreach(println)
}
