package fr.ippon.dojo.spark

import org.apache.spark.sql.SparkSession

object Exercice4SparkStreaming {
  def main(args:Array[String]) {

    // Créer la SparkSession
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Coding Dojo - Exercice 4")
      .getOrCreate

    // Utilisé pour faire des conversions implicites
    import spark.implicits._

    // Charger le stream
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    // Afficher les données du stream
    val query = lines.map(row => row.getString(0).split(" "))
      .map(row => Names(row(0), row(1), Integer.valueOf(row(2)), Integer.valueOf(row(3)), Integer.valueOf(row(4))))
      .writeStream
      .format("console")
      .start()

    // Exécute la requete de streaming
    query.awaitTermination()

    // Aggréger le nombre de naissance par département
    val query2 = lines.map(row => row.getString(0).split(" "))
      .map(row => Names(row(0), row(1), Integer.valueOf(row(2)), Integer.valueOf(row(3)), Integer.valueOf(row(4))))
      .groupBy("dpt")
      .sum("nombre")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Exécute la requete de streaming
    query2.awaitTermination()

  }
}
