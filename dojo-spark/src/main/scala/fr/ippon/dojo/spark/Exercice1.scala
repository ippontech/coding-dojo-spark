package fr.ippon.dojo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Exercice 1
  * Cas d'utilisation des RDDs sur le jeu de données des prénoms
  */
object Exercice1 {

  def main(args: Array[String]) {

    // configuration du job Spark
    val conf = new SparkConf()
      .setAppName("Coding Dojo Data - Exercice 1")
      .setMaster("local")

    // instanciation du Spark Context


    // chargez le fichier dans un RDD


    // faites un comptage du nombre de ligne et affichez le dans le terminal


    // affichez les 20 premiers éléments


    // créez une case class en dehors de la méthode main avec le nom des colonnes du header

    // créez un nouveau rdd en supprimant la première ligne
    // puis découpez les autres par le délimiteur
    // et appliquez votre tableau de string dans la case class que vous avez créé


    // mettez ce RDD en cache


    // afficher les 20 premières lignes de ce nouveau RDD


    // trouvez le nombre de personne qui ont le même prénom que vous


    // êtes-vous le seul à avoir eu votre prénom l'année de votre naissance dans votre département ?


    // déterminez le nombre total de prénoms différents


    // trouvez le top 10 des prénoms utilisés pour l'année de votre naissance
    // vous pouvez créer un tuple en étape intermédiaire (String, Float)


  }
}
