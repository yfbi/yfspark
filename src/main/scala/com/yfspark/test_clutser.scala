package com.yfspark

import org.apache.spark.{SparkConf, SparkContext}

object test_clutser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("First Clutser Spark App")


//    conf.setMaster("local") //1.此时，程序在本地运行，无需SPARK集群

//    conf.set("fs.defaultFS", "hdfs://localhost:9000");//2.集群配置，在配置文件conf中指定所用的文件系统---HDFS
//    conf.setMaster("spark://localhost:9000").set("spark.ui.port","9000")

//    System.setProperty("hadoop.home.dir", "D://hadoop//hadoop-2.6.5");
    val sc = new SparkContext(conf)
    var wordCounts=sc.makeRDD( List(     (111,(1,((("1,2,3","a",3),("1,2","a",2)))))     ) )

    wordCounts.collect().foreach(println)


    sc.stop();


  }
}