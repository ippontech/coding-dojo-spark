package fr.ippon.dojo.spark

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

/**
  * US#3-4 : Cassandra
  */
object Cassandra {

  val PATH: String = "/home/dojo/workspace/coding-dojo-spark/"
  val FILE_PATH: String = PATH + "/data/enseignement/fr-esr-atlas_regional-effectifs-d-etudiants-inscrits.csv"
  val KEYSPACE: String = "ippon_technologies"
  val TABLE: String = "effectifs"

  case class Workforce(
                        uuid: String,
                        rentree: Int,
                        rentree_univ: String,
                        niveau_geo: String,
                        unite_geo: String,
                        code_postal: String,
                        regroupement_code: String,
                        regroupement: String,
                        secteur_code: String,
                        secteur: String,
                        sexe_code: Int,
                        sexe: String,
                        nb_inscrits: Double)

  def toDouble(d: Any): Double = if (d == null) 0d else d.toString.toDouble;

  def loadCassandraTable(spark: SparkSession, df: DataFrame): Unit = {
    val writeToCassandra = spark.sqlContext.createDataFrame(df.rdd.map(row => Workforce(
      UUID.randomUUID().toString,
      row.getInt(0),
      row.getString(1),
      row.getString(2),
      row.getString(3),
      row.getString(20),
      row.getString(4),
      row.getString(5),
      row.getString(6),
      row.getString(7),
      row.getInt(8),
      row.getString(9),
      toDouble(row.get(10))
    ))).write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> TABLE, "keyspace" -> KEYSPACE))
      .mode(SaveMode.Overwrite)
      .save()
  }

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
    val dfEffectives = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(FILE_PATH)

    // dfEffectives.printSchema()
    // dfEffectives.show()

    //
    // Top 10 des regroupements des communes ayant eu le plus d'inscrits pour la rentrée 2015
    //
    val nbEffectives = dfEffectives.where(dfEffectives("rentrée") === 2015)
      .where(dfEffectives("niveau_geo") === "COMMUNE")
      .where(dfEffectives("regroupement") =!= "TOTAL")
      .groupBy(dfEffectives("regroupement"), dfEffectives("Identifiant de l’unité géographique"))
      .sum("Nombre total d’étudiants inscrits").as("nbEffectives")

    val top10 = nbEffectives.sort(nbEffectives("sum(Nombre total d’étudiants inscrits)").desc)
      .limit(10)
      .show()

    // Insert data cassandra
    // loadCassandraTable(spark, dfEffectives)

    // Read from cassandra
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> TABLE, "keyspace" -> KEYSPACE))
      .load()

    // df.show()
    // df.printSchema()

    //
    // Classement des regroupements par effectifs pour chaque commune ayant au moins 3 établissements
    //
    val rankingByMunicipality = df.where(df("niveau_geo") === "Commune")
      .where(df("regroupement") =!= "TOTAL")
      .withColumnRenamed("nb_inscrits", "effectifs")
    rankingByMunicipality.cache()

    val rankingEffectives = rankingByMunicipality
      .select(rankingByMunicipality("regroupement"), rankingByMunicipality("code_postal"), rankingByMunicipality("niveau_geo"), rankingByMunicipality("effectifs"))
      .groupBy(rankingByMunicipality("regroupement"), rankingByMunicipality("code_postal"))
      .agg(functions.sum(rankingByMunicipality("effectifs")).as("sum"))
      .withColumn("rank", functions.row_number().over(Window.partitionBy("code_postal").orderBy(functions.desc("sum"))))

    val nbLocationsByMunicipality = rankingByMunicipality
      .select(rankingByMunicipality("regroupement"), rankingByMunicipality("code_postal"))
      .groupBy(rankingByMunicipality("code_postal"))
      .agg(functions.countDistinct(rankingByMunicipality("regroupement")).as("nbLocationsByMunicipality"))

    val results = nbLocationsByMunicipality.where(nbLocationsByMunicipality("nbLocationsByMunicipality") >= 3)
      .join(rankingEffectives, rankingEffectives.col("code_postal") === nbLocationsByMunicipality.col("code_postal"))
      .show()


    //
    // Effectifs moyen par type établissement pour les communes ayant au moins un établissement public et un établissement privé
    //
    val averageEffective = ???

    spark.stop()
  }
}
