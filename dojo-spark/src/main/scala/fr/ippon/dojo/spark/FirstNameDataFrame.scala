package fr.ippon.dojo.spark

import org.apache.spark.sql.{SparkSession, functions}

/**
  * US#2 : DataFrames
  */
object FirstNameDataFrame {

  val PATH: String = "/home/dojo/workspace/coding-dojo-spark/"
  val FILE_PATH: String = PATH + "/data/insee/dpt2015.txt"

  def main(args: Array[String]) {

    // Spark configuration
    val spark = SparkSession
      .builder
      .appName("Dojo Spark-Cassandra [FirstNameDataFrame]")
      .master("local[*]")
      .getOrCreate()

    // Load file
    val df = spark.read
      .option("header", true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv(FILE_PATH)

    // df.cache()
    // df.show()

    //
    // Nombre de prénoms différents
    //
    val nbFirstName = ???

    // System.out.println("Nb FirstName : " + nbFirstName.count())

    //
    // Top 10 des prénoms de l'année 2010
    //
    val top10FirstName = ???

    spark.stop()
  }
}
