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

  val KEYSPACE = ???
  val TABLE = ???

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


    // affichez le schéma


    // affichez un extrait des données pour analyser le jeu de données


    // Convertissez en booléen la colonne diffusable au moyen de la méthode when de functions)
    // dans une nouvelle colonne


    // Dans une méthode select :
    // 1) ajouter une colonne pour l'identifiant UUID (donné)
    // 2) renommez toutes les colonnes
    //    pour qu'elles correspondent à votre schéma de table cassandra (utilisez la méthode col de functions)
//    val formatDf = inscriptionsDf
//      .select(
//        generateUUID() as "uuid",
//
//      )

    // écrivez votre dataframe dans la table cassandra que vous avez créé pour
    // utilisez le save mode overwrite


    // lisez maintenant le dataframe que vous venez d'écrire
//    val cassandraDf = spark
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> TABLE, "keyspace" -> KEYSPACE))
//      .load

    // afficher son schéma


    // affichez les 20 premiers éléments


    // 1) trouvez le top 10 des regroupements des communes ayant eu le plus d'inscrits pour la rentrée 2015
    // donc différents des regroupement totaux
    // 2) ajoutez ensuite un compteur qui s'incrémente dans une nouvelle colonne (en partant de 1 et non de 0)



    // Déterminez un classement des regroupements par effectifs pour chaque commune ayant au moins 3 établissements :
    // 1) Pour cela, restreignez les données sur les communes ne contenant pas de regroupement totaux


    // 2) Calculez le nombre d'inscrits totaux pour chaque regroupement et code postal
    // 3) Ajoutez au DataFrame résultant une nouvelle colonne donnant le ranking des regroupements
    //    pour chaque code postal en se basant sur le total d'inscriptions calculé
    //    - créez une fenêtre glissante partitionnée par le code_postal (Object Window)
    //    - triez cette fenêtre glissante sur les valeurs de total décroissantes
    //    - utilisez la fonction row_number de l'Object functions sur la fenêtre glissante pour déterminer le ranking


    // 4) Calculez dans un nouveau DataFrame le nombre de regroupement par code postal
    // 5) Gardez seulement les codes postaux avec au minimum 3 regroupements


    // 6) Joignez les 2 DataFrames que vous avez créé aux étapes 3) et 5)
    // 7) retirez une des deux colonnes ayant servi pour la jointure et affichez le résultat


    // Optionel
    // Effectifs moyen par type établissement pour les communes ayant au moins un établissement public et un établissement privé

    spark.stop
  }
}
