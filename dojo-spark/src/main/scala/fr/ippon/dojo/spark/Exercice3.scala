package fr.ippon.dojo.spark

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Exercice 3
  * Cas d'utilisation des DataFrames / DataSets avec Cassandra sur le jeu de données des inscriptions
  */
object Exercice3 {

  val KEYSPACE = "dojo"
  val TABLE = "effectifs"

  val generateUUID = udf(() => UUID.randomUUID().toString)

  def main(args: Array[String]) {

    // Spark configuration
    val spark = SparkSession
      .builder
      .appName("Coding Dojo Data - Exercice 3")
      .master("local")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .getOrCreate

    // chargez le fichier dans un dataframe (pensez à ajouter l'option d'inférence de schéma)
    val inscriptionsDf = spark
      .read
      .option("header", true)
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/inscriptions/inscriptions_etudiants.csv")

    // affichez le schéma
    println("\nSchéma du DataFrame initial :")
    inscriptionsDf.printSchema

    // affichez un extrait des données pour analyser le jeu de données
    println("\nExtrait du DataFrame initial :")
    inscriptionsDf.show

    // Convertissez en booléen la colonne diffusable au moyen de la méthode when de functions)
    // dans une nouvelle colonne
    println("\nConversion en booléen d'une colonne :")
    inscriptionsDf
      .select(
        col("diffusable")
        , when(col("diffusable") === "oui", true).otherwise(false).as("boolean_diffusable"))
      .distinct
      .show

    // Dans une méthode select :
    // 1) ajouter une colonne pour l'identifiant UUID (donné)
    // 2) renommez toutes les colonnes
    //    pour qu'elles correspondent à votre schéma de table cassandra (utilisez la méthode col de functions)
    val formatDf = inscriptionsDf
      .select(
        generateUUID() as "uuid"
        , col("rentrée").as("rentree")
        , col("Rentrée universitaire").as("rentree_univ")
        , col("Niveau géographique").as("niveau_geo")
        , col("Unité géographique").as("unite_geo")
        , col("Identifiant de l’unité géographique").as("code_postal")
        , col("regroupement").as("regroupement_code")
        , col("Regroupements de formations ou d’établissements").as("regroupement")
        , col("secteur").as("secteur_code")
        , col("Secteur de l’établissement d’inscription").as("secteur")
        , col("sexe").as("sexe_code")
        , col("Sexe de l’étudiant").as("sexe")
        , col("Nombre total d’étudiants inscrits").as("nb_inscrits"))

    println("\nDataFrame formaté :")
    formatDf.show

    // écrivez votre dataframe dans la table cassandra que vous avez créé pour
    // utilisez le save mode overwrite
    formatDf
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> TABLE, "keyspace" -> KEYSPACE))
//      .mode(SaveMode.Overwrite)
//      .save

    // lisez maintenant le dataframe que vous venez d'écrire
    val cassandraDf = formatDf
//    val cassandraDf = spark
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> TABLE, "keyspace" -> KEYSPACE))
//      .load

    // afficher son schéma
    println("\nLe schéma du DataFrame écrit sur Cassandra")
    cassandraDf.printSchema

    // affichez les 20 premiers éléments
    println("\nLes 20 premiers éléments du DataFrame depuis Cassandra :")
    cassandraDf.show

    // 1) trouvez le top 10 des regroupements des communes ayant eu le plus d'inscrits pour la rentrée 2015
    // donc différents des regroupement totaux
    // 2) ajoutez ensuite un compteur qui s'incrémente dans une nouvelle colonne (en partant de 1 et non de 0)
    println("\nTop 10 des regroupements de communes :")
    cassandraDf
      .where(
        col("rentree") === 2015
          && col("niveau_geo") === "Commune"
          && col("regroupement") =!= "TOTAL")
      .groupBy(col("regroupement"), col("code_postal"))
      .agg(sum("nb_inscrits").as("total"))
      .sort(col("total").desc)
      .withColumn("ranking", monotonically_increasing_id + 1)
      .limit(10)
      .show


    // Déterminez un classement des regroupements par effectifs pour chaque commune ayant au moins 3 établissements :
    // 1) Pour cela, restreignez les données sur les communes ne contenant pas de regroupement totaux
    val rankingByMunicipality = cassandraDf
      .select("regroupement", "code_postal", "niveau_geo", "nb_inscrits")
      .where(
        col("niveau_geo") === "Commune"
          && col("regroupement") =!= "TOTAL")
      .cache

    // 2) Calculez le nombre d'inscrits totaux pour chaque regroupement et code postal
    // 3) Ajoutez au DataFrame résultant une nouvelle colonne donnant le ranking des regroupements
    //    pour chaque code postal en se basant sur le total d'inscriptions calculé
    //    - créez une fenêtre glissante partitionnée par le code_postal (Object Window)
    //    - triez cette fenêtre glissante sur les valeurs de total décroissantes
    //    - utilisez la fonction row_number de l'Object functions sur la fenêtre glissante pour déterminer le ranking
    val rankingEffectives = rankingByMunicipality
      .groupBy("regroupement", "code_postal")
      .agg(sum(col("nb_inscrits")).as("total"))
      .withColumn("ranking", row_number
        .over(Window
          .partitionBy("code_postal")
          .orderBy(desc("total"))))

    // 4) Calculez dans un nouveau DataFrame le nombre de regroupement par code postal
    // 5) Gardez seulement les codes postaux avec au minimum 3 regroupements
    val nbLocationsByMunicipality = rankingByMunicipality
      .groupBy("code_postal")
      .agg(countDistinct("regroupement").as("locationsPerTown"))
      .where(col("locationsPerTown") >= 3)

    // 6) Joignez les 2 DataFrames que vous avez créé aux étapes 3) et 5)
    // 7) retirez une des deux colonnes ayant servi pour la jointure et affichez le résultat
    nbLocationsByMunicipality
      .join(rankingEffectives, nbLocationsByMunicipality("code_postal") === rankingEffectives("code_postal"))
      .drop(rankingEffectives("code_postal"))
      .show

    // Optionel
    // Effectifs moyen par type établissement pour les communes ayant au moins un établissement public et un établissement privé
//    val averageEffective = ???

    spark.stop
  }
}
