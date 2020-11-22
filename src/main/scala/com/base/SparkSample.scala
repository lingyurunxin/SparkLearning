package com.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkSample {

    def main(args: Array[String]): Unit = {

        val sampleConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Sample")
        val context: SparkContext = new SparkContext(sampleConf)
        val listRDD: RDD[Int] = context.makeRDD(1 to 10)
        val sampleRDD: RDD[Int] = listRDD.sample(false, .4, 1)
        sampleRDD.collect().foreach(println)
    }

}
