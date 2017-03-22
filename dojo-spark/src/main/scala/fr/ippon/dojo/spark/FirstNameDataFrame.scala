package fr.ippon.dojo.spark

import org.apache.spark.sql.{SparkSession, functions}

object FirstNameDataFrame {

  private var path: String = "/home/dojo/workspace/coding-dojo-spark/"
  private var filePath: String = path + "/data/insee/dpt2015.txt"

  def main(args: Array[String]) {

    // Spark configuration
    val spark = SparkSession
      .builder
      .appName("Dojo Spark-Cassandra [FirstNameDataFrame]")
      .master("local[*]")
      .getOrCreate()

    // Load file
    val df = spark.read.option("header", true).option("delimiter", "\t").csv(filePath)
    df.cache()
    df.show()

    // Nombre de prénoms
    val nbFirstName = df.where(!df("preusuel").startsWith("_"))
      .select(df("preusuel"))
      .distinct()
    //System.out.println("Nb FirstName : " + nbFirstName.count())

    // Top 10 prénoms de l'année 2010
    val sumFirstName = df.where(!df("preusuel").startsWith("_") && df("annais") === "2010")
      .groupBy(df("preusuel"))
      .agg(functions.sum(df("nombre")).as("sum"))
    sumFirstName.sort(sumFirstName("sum").desc)
      .show(10)

    spark.stop()
  }
}
