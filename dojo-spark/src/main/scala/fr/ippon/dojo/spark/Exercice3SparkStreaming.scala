package fr.ippon.dojo.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercice3SparkStreaming {
  def main(args:Array[String]) {

    // Création de la configuration
    val sparkConf = new SparkConf()
      .setAppName("Coding Dojo - Exercice 3")
      .setMaster("local[2]")

    // Créer le Streaming Context
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Créer le DStream à partir d'un socket
    val lines = ssc.socketTextStream("localhost",9999)

    // Afficher le contenu du stream
    lines.print()

    // Afficher le contenu du stream
    lines.foreachRDD(rdd => rdd.foreach(println))

    // Enregistrer le contenu du stream dans des fichiers texte
    lines.saveAsTextFiles("C://Dev//dpt2016//test")

    // Exécuter le job de Streaming
    ssc.start()
    ssc.awaitTermination()

  }
}
