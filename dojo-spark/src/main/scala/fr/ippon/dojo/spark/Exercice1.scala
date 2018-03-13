package fr.ippon.dojo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Exercice 1
  * Cas d'utilisation des RDDs sur le jeu de données des prénoms
  */
object Exercice1 {

  case class Prenom(sexe: String, preusuel: String, annais: String, dpt: String, nombre: Int)

  def main(args: Array[String]) {

    // configuration du job Spark
    val conf = new SparkConf()
      .setAppName("Coding Dojo Data - Exercice 1")
      .setMaster("local")

    // instanciation du Spark Context
    val sc = new SparkContext(conf)

    // chargez le fichier dans un RDD
    val initialFirstNameRdd = sc.textFile("src/main/resources/data/insee/dpt2016.txt")

    // faites un comptage du nombre de ligne et affichez le dans le terminal
    println("comptage initial = " + initialFirstNameRdd.count)

    // affichez les 20 premiers éléments
    println("\nRDD initial")
    initialFirstNameRdd
      .take(20)
      .foreach(println)

    // créez une case class en dehors de la méthode main avec le nom des colonnes du header

    // créez un nouveau rdd en supprimant la première ligne
    // puis découpez les autres par le délimiteur
    // et appliquez votre tableau de string dans la case class que vous avez créé
    val firstNameRdd = initialFirstNameRdd
      .filter(s => !s.startsWith("sexe"))
      .map(line => (line.split("\t")))
      .filter(_.length > 1)
      .map(k => Prenom(k.head, k(1), k(2), k(3), k.last.toInt))

    // mettez ce RDD en cache
    val cachedFirstNameRdd = firstNameRdd.cache

    // afficher les 20 premières lignes de ce nouveau RDD
    println("\nRDD formaté :")
    cachedFirstNameRdd
      .take(20)
      .foreach(println)


    // trouvez le nombre de personne qui ont le même prénom que vous
    println("\nNombre de personnes pour le nom donné : " + cachedFirstNameRdd
      .filter(_.preusuel == "ANTOINE")
      .map(_.nombre)
      .sum)

    // êtes-vous le seul à avoir eu votre prénom l'année de votre naissance dans votre département ?
    println("\nNombre de personnes pour un nom, un département et une année donnés : " + cachedFirstNameRdd
      .filter(_.preusuel == "ANTOINE")
      .filter(_.dpt == "59")
      .filter(_.annais == "1990")
      .map(_.nombre)
      .collect()(0))

    // déterminez le nombre total de prénoms différents
    println("\nNombre total de prénoms différents : " + cachedFirstNameRdd
      .map(_.preusuel)
      .distinct
      .count)

    // trouvez le top 10 des prénoms utilisés pour l'année de votre naissance
    // vous pouvez créer un tuple en étape intermédiaire (String, Float)
    println("\nTop 10 des prénoms utilisés pour une année donnée :")
    cachedFirstNameRdd
      .filter(_.annais == "1990")
      .map(l => (l.preusuel, l.nombre.toFloat))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)

    sc.stop
  }
}
