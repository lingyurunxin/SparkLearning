package com.liyuanfeng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDistinct {
    def main(args: Array[String]): Unit = {

        val distinct: SparkConf = new SparkConf().setMaster("local[*]").setAppName("distinct")
        val context: SparkContext = new SparkContext(distinct)
        val value: RDD[Int] = context.makeRDD(List(1, 2, 1, 1, 2, 4))
        println(value.partitions.length)
        value.distinct().foreach(println)

        value.distinct(2).saveAsTextFile("out")

    }

}
