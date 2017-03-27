package fr.ippon.dojo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * US#1 : Rdds
  */
object Exercice1 {

  val PATH: String = "/home/dojo/workspace/coding-dojo-spark/"
  val FILE_PATH: String = PATH + "/data/insee/dpt2015.txt"
  val SEPARATOR: String = "\t";

  def main(args: Array[String]) {

    // Spark configuration
    val conf = new SparkConf()
      .setAppName("Dojo Spark-Cassandra [FirstNameRdd]")
      .setMaster("local[*]")
    // Spark context
    val sc = new SparkContext(conf)

    // Load file
    val rdd = sc.textFile(FILE_PATH)

    // rdd.cache()

    //
    // Nombre de prénoms différents
    //
    val nbFirstName = rdd.filter(s => !s.startsWith("sexe"))
      .map(line => line.split(SEPARATOR))
      .filter(x => !x(1).startsWith("_"))
      .map(x => x(1))
      .distinct()


    // System.out.println("Nb FirstName : " + nbFirstName.count())

    //
    // Top 10 prénoms de l'année 2010
    //
    val top10FirstName = rdd.filter(s => !s.startsWith("sexe"))
      .map(line => line.split(SEPARATOR))
      .filter(x => x(2) == "2010" && !x(1).startsWith("_"))
      .map(x => (x(1), x(4).toFloat))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)


    sc.stop()
  }
}

// scalastyle:on println
