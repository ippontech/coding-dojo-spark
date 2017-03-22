package fr.ippon.dojo.spark

import org.apache.spark.sql.SparkSession

object FirstNameDataFrame {

  //private var path: String = "/home/dojo/workspace/coding-dojo-spark/"
  private var path: String = "/Users/thomascozien/Dev/ippon/coding-dojo-spark"
  private var filePath: String = path + "/data/insee/dpt2015.txt"
  private var separator: String = "\t";

  def main(args: Array[String]) {

    // Spark configuration
    val spark = SparkSession
      .builder
      .appName("Dojo Spark-Cassandra [FirstNameDataFrame]")
      .master("local[*]")
      .getOrCreate()

    // Load file
    val df = spark.read.option("header", true).option("delimiter", separator).csv(filePath)
    df.cache();

    spark.stop()
  }
}
