package fr.ippon.dojo.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Exercice 2
  * Cas d'utilisation des DataFrames / DataSets sur le jeu de données des prénoms
  */
object Exercice2 {

  def main(args: Array[String]) {

    // instanciation du Spark Session
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Coding Dojo - Exercice 2")
      .getOrCreate

    // chargez le fichier dans un DataFrame ou DataSet et mettez le en cache
    val firstNameDf = spark
      .read
      .option("header", true)
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/insee/dpt2015.txt")
      .cache

    // faites un comptage du nombre de ligne et affichez le dans le terminal
    println("nombre de lignes du fichier : " + firstNameDf.count)

    // affichez le schéma du dataframe
    println("\nSchéma des données :")
    firstNameDf.printSchema

    // affichez les 20 premiers éléments
    println("\nExtrait de données :")
    firstNameDf.show

    // trouvez le nombre de personne qui ont le même prénom que vous
    println("\nNombre de personnes avec un même prénom donné :")
    firstNameDf
      .where(col("preusuel") === "ANTOINE")
      .groupBy("preusuel")
      .sum("nombre")
      .show

    // êtes-vous le seul à avoir eu votre prénom l'année de votre naissance dans votre département ?
    println("\nNombre de personnes pour un nom, un département et une année donnés :")
    firstNameDf
      .where(
        col("preusuel") === "ANTOINE"
          && col("dpt") === "59"
          && col("annais") === "1990")
      .show

    // déterminez le nombre total de prénoms différents
    println("\nNombre de prénoms différents : " + firstNameDf
      .select("preusuel")
      .distinct
      .count)

    // trouvez le top 10 des prénoms utilisés pour l'année de votre naissance
    // vous pouvez caster le nombre en Float
    val top10FirstNameDf = firstNameDf
      .where(col("annais") === "1990")
      .select(col("preusuel"), col("nombre"))
      .groupBy("preusuel")
      .agg(sum("nombre").as("sum"))
      .orderBy(col("sum").desc)
      .limit(10)

    println("\nTop10 des prénoms pour une année donnée :")
    top10FirstNameDf.show

    // affichez le plan d'exécution de ce dernier dataframe
    println("\nPlan d'exécution du DataFrame :")
    top10FirstNameDf.explain

    spark.stop
  }
}
