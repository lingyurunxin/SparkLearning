package com.actioin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
    def main(args: Array[String]): Unit = {

        val countByKey: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CountByKey")
        val context: SparkContext = new SparkContext(countByKey)
        val rdd: RDD[(String, Any)] = context.parallelize(List(("a", 2), ("b", 3), ("c", 1), ("a", "1")))
        val stringToLong: collection.Map[String, Long] = rdd.countByKey()
        println(stringToLong.mkString(","))
        println(stringToLong)
        stringToLong.foreach((item: (String, Long)) => {
            println(item._1 + "：" + item._2)
        })


    }

}
