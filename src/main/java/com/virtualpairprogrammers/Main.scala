package com.virtualpairprogrammers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]")
    val sc = SparkSession.builder()
      .config(conf)
      .getOrCreate()
      .sparkContext
    val initialRdd = sc.textFile("src/main/resources/subtitles/input.txt")
    val lettersOnlyRdd = initialRdd.map((sentence: String) => sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase)
    val removedBlankLines = lettersOnlyRdd.filter((sentence: String) => sentence.trim.length > 0)
    val justWords = removedBlankLines.flatMap((sentence: String) => sentence.split(" "))
    val blankWordsRemoved = justWords.filter((word: String) => word.trim.length > 0)
    val justInterestingWords = blankWordsRemoved.filter(Util.isNotBoring)
    val pairRdd = justInterestingWords.map((word: String) => new Tuple2[String, Long](word, 1L))
    val totals = pairRdd.reduceByKey((value1, value2) => value1 + value2)

    val switched = totals.map((tuple: Tuple2[String, Long]) => new Tuple2[Long, String](tuple._2, tuple._1))
    val sorted = switched //.sort(false)
    val results = sorted.take(10)
    print(results.head)
  }
}