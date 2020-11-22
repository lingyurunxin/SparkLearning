package com.actioin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
        val context: SparkContext = new SparkContext(sparkConf)
        val rdd: RDD[Int] = context.makeRDD(1 to 10)

        val result: Int = rdd.aggregate(10)(_ + _, _ + _)//前一个参数是分区内相加，后一个参数是分区间相加，zeroValue是初始值，aggregate函数相加的时候，分区内加分区间也加
        println(rdd.partitions.length)
        println(result)

    }
}
