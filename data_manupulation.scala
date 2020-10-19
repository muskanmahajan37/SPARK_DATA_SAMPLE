package com.FirstProgramme
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object data_manupulation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("C:\\Users\\ag16000\\Desktop\\GIT\\PRACTISE\\SCALA_PRACTISE\\FirstScalaProgramme\\src\\main\\scala\\com\\FirstProgramme\\temp.txt")
    val counts = text.flatMap(line => line.split(" ")
    ).map(word => (word,1)).reduceByKey(_+_)
    val col = counts.collect
    println(col)
  }
}


