package com.yfspark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount_clutser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("First Clutser Spark App")


//    conf.setMaster("local") //1.此时，程序在本地运行，无需SPARK集群

    conf.set("fs.defaultFS", "hdfs://192.168.4.32:9000");//2.集群配置，在配置文件conf中指定所用的文件系统---HDFS
//    conf.set("fs.defaultFS","hdfs://SRV-Apollo-Node1.YFDYF.Biz:9000")

//    System.setProperty("hadoop.home.dir", "D://hadoop//hadoop-2.6.5");
    val sc = new SparkContext(conf)

    /*
     * 第三步：根据具体的数据源(HDFS\HBASE\LOCAL FS\DB等)通过SPARKContext来创建RDD
     * RDD的创建基本有三种方式：
     * 1.根据外部的数据来源，如HDFS
     * 2.根据SCALA集合
     * 3.由其他的RDD操作数据会被RDD划分成一些列的Partitions，分配到每个Partitions的数据
     * 属于一个Task的处理范畴
     * 
     */

    //本地文件  val lines = sc.textFile("D://test.txt", 1)//path指的是文件路径，minPartitions=1指的是最小并行度hdfs:/localhost:9000
    val lines = sc.textFile("hdfs://192.168.4.32:9000/test/test.txt")//HDFS上的文件，不需要指定并发度，最小有默认

    /*
     * 第四部：
     * 对初始的RDD进行Transformation级别的处理，
     * 例如map\filter等高阶函数等的编程，来进行具体的数据计算
     * 第4.1步：将每一行的字符串拆分成单个的单词
     */
    val words =lines.flatMap { line => line.split(" ") }


    /*
     * 4.2步：
     * 在单词拆分的基础上对每个单词实例计数为1，也就是
     * word => (word,1)
     */
    val pairs = words.map { word  => (word,1) }

    /*
     * 4.3步：在每个单词实例计数为1的基础上统计每个单词在
     * 文件中出现的总次数
     */

    val wordCounts = pairs.reduceByKey(_+_)

    wordCounts.collect().foreach(wordNumberPair => println(wordNumberPair._1 + ":" +wordNumberPair._2 ))

    /*
     * 第5步
     */

    sc.stop();


  }
}