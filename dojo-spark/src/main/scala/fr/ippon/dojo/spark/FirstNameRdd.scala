/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package fr.ippon.dojo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * US#1 : Rdds
  */
object FirstNameRdd {

  private var path: String = "/home/dojo/workspace/coding-dojo-spark/"
  private var filePath: String = path + "/data/insee/dpt2015.txt"
  private var separator: String = "\t";

  def main(args: Array[String]) {

    // Spark configuration
    val conf = new SparkConf()
      .setAppName("Dojo Spark-Cassandra [FirstNameRdd]")
      .setMaster("local[*]")
    // Spark context
    val sc = new SparkContext(conf)

    // Load file
    val rdd = sc.textFile(filePath)

    // rdd.cache()

    //
    // Nombre de prénoms différents
    //
    val nbFirstName = ???

    /*
    val nbFirstName = rdd.filter(s => !s.startsWith("sexe"))
      .map(line => line.split(separator))
      .filter(x => !x(1).startsWith("_"))
      .map(x => x(1))
      .distinct()
     */

    // System.out.println("Nb FirstName : " + nbFirstName.count())

    //
    // Top 10 prénoms de l'année 2010
    //
    val top10FirstName = ???

    /*
    val top10FirstName = rdd.filter(s => !s.startsWith("sexe"))
      .map(line => line.split(separator))
      .filter(x => x(2) == "2010" && !x(1).startsWith("_"))
      .map(x => (x(1), x(4).toFloat))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)
     */

    sc.stop()
  }
}

// scalastyle:on println
