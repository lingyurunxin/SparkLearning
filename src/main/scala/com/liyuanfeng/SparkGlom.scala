package com.liyuanfeng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

/**
 * @Desc:
 * @Author: liyuanfeng@qianxin.com
 * @Date: 2020-11-14 16:48
 */

object SparkGlom {

    def main(args: Array[String]): Unit = {
        val glom: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Glom")
        val context: SparkContext = new SparkContext(glom)
        val value: RDD[Int] = context.makeRDD(1 to 16, 4)
        val glomRDD: RDD[Array[Int]] = value.glom()
        glomRDD.collect().foreach(array=>{
            println(array.mkString(","))
        })

    }

}
