package com.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val context: SparkContext = new SparkContext(sparkConf)


        val lines: RDD[String] = context.textFile("src/main/resources")

        val value: RDD[(String, Int)] = lines.flatMap((_: String).split(" ")).map(((_: String), 1)).reduceByKey((_: Int) + (_: Int))

        println(value.collect().mkString("Array(", ", ", ")"))

        val value1: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        println(value1.collect().mkString(","))

    }

}
