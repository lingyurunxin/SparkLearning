package com.liyuanfeng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkGroupBy {
    def main(args: Array[String]): Unit = {

        val groupBy: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy")
        val context: SparkContext = new SparkContext(groupBy)
        val value: RDD[Int] = context.makeRDD(1 to 10)
        val groupByRDD: RDD[(Int, Iterable[Int])] = value.groupBy((_: Int) % 2)
        groupByRDD.collect().foreach(println)

    }

}
