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
    val nbFirstName = ???

    // System.out.println("Nb FirstName : " + nbFirstName.count())

    //
    // Top 10 prénoms de l'année 2010
    //
    val top10FirstName = ???

    sc.stop()
  }
}

// scalastyle:on println
