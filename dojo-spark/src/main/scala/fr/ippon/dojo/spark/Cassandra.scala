package fr.ippon.dojo.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}

/**
  * US#3 : Cassandra
  */
object Cassandra {

  //private var path: String = "/home/dojo/workspace/coding-dojo-spark/"
  private var path: String = "/Users/thomascozien/Dev/ippon/coding-dojo-spark/"
  private var filePath: String = path + "/data/enseignement/fr-esr-atlas_regional-effectifs-d-etudiants-inscrits.csv"

  def main(args: Array[String]) {

    // Spark configuration
    val spark = SparkSession
      .builder
      .appName("Dojo Spark-Cassandra [Cassandra]")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .getOrCreate()

    // Load file
    val df = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(filePath)

    df.cache()
    df.show()
    df.printSchema()

    // Insert into cassandra

    // Load from cassandra
    //    val df = spark
    //      .read
    //      .format("org.apache.spark.sql.cassandra")
    //      .options(Map( "table" -> "effectifs", "keyspace" -> "ippon_technologies"))
    //      .load()

    //
    // Classement des regroupements par effectifs pour chaque commune ayant au moins 3 établissements
    //
    val rankingByMunicipality = ???
    /*
    val rankingByMunicipality = df.where(df("niveau_geo") === "COMMUNE")
      .where(df("regroupement") =!= "TOTAL")
      .withColumnRenamed("Nombre total d’étudiants inscrits", "effectifs")
    rankingByMunicipality.cache()

    val workforce = rankingByMunicipality
      .select(rankingByMunicipality("regroupement"), rankingByMunicipality("Identifiant de l’unité géographique"), rankingByMunicipality("niveau_geo"), rankingByMunicipality("effectifs"))
      .groupBy(rankingByMunicipality("regroupement"), rankingByMunicipality("Identifiant de l’unité géographique"))
      .agg(functions.sum(rankingByMunicipality("effectifs")).as("sum"))
      .withColumn("rank", functions.row_number().over(Window.partitionBy("Identifiant de l’unité géographique").orderBy(functions.desc("sum"))))

    val nbLocationsByMunicipality = rankingByMunicipality
      .select(rankingByMunicipality("regroupement"), rankingByMunicipality("Identifiant de l’unité géographique"))
      .groupBy(rankingByMunicipality("Identifiant de l’unité géographique"))
      .agg(functions.countDistinct(rankingByMunicipality("regroupement")).as("nbLocationsByMunicipality"))

    val results = nbLocationsByMunicipality.where(nbLocationsByMunicipality("nbLocationsByMunicipality") >= 3)
      .join(workforce, workforce.col("Identifiant de l’unité géographique") === nbLocationsByMunicipality.col("Identifiant de l’unité géographique"))
      .show()
     */

    //
    // Effectifs moyen par type établissement pour les communes ayant au moins un établissement public et un établissement privé
    //
    // val averagePeople = ???

    spark.stop()
  }
}
