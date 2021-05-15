package com.jk.spark.FirstTask

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {


  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val input =  sc.textFile("input.txt")
    val output = "output_wordcount"

    //Transform 1
    val words = input.flatMap(line => line.split("\\W+"))
    println("After flatmap split by words:")
    words.foreach(f=>println(f))

    //Transform 2
    val counts = words.map(words => (words, 1)).reduceByKey(_+_,1)
    println("map output:")
    counts.foreach(f=>println(f))

    //Transform 3
    val wordsList=counts.sortBy(outputLIst=>outputLIst._1,ascending = true)
    println("Sorted Order:")
    wordsList.foreach(outputLIst=>println(outputLIst))

    //Transform 4
    val wordsFilter = wordsList.filter(outputList=> outputList._2 >1)
    println("Filtered output:")
    wordsFilter.foreach(outputList=>println(outputList))

    wordsFilter.saveAsTextFile(output)

    //Action 1
    wordsFilter.take(5).foreach(outputLIst=>println(outputLIst))
    //Action 2
    println("Count Unique words:",wordsFilter.count())

    sc.stop()

  }


}
