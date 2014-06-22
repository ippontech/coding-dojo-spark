# Coding Dojo - Apache Spark & Open Data

## Ressources

* [Spark](http://spark.apache.org/)
* [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html)
* [Making Apache Spark Easier to Use in Java with Java 8](http://blog.cloudera.com/blog/2014/04/making-apache-spark-easier-to-use-in-java-with-java-8/)
* [Spark Standalone Mode](http://spark.apache.org/docs/latest/spark-standalone.html)

## Données utilisées

### Open Data Paris - Arbres alignements

Les quelques 110 000 arbres d'alignements de Paris avec leur type et leur emplacement géographique.

Fichier CSV, un record par arbre.

### Open Data Paris - Tonnage déchets bacs jaunes

Tableau croisé du tonnage des déchets en bacs jaunes, par arrondissement et par mois, sur l'année 2011.

Fichier CSV, un record par arrondissement, une colonne par mois.

### Wikipedia Pagecounts

Statistiques de pages vues :

* par "projet Wikipedia"
* aggrégées par heure
* format : projet page nb_visites volume_donnees

Deux datasets :

* wikipedia-pagecounts-days : 5 fichiers de statistiques du dimanche à minuit
* wikipedia-pagecounts-hours : les 24 fichiers d'une journée complète

Source : [Page view statistics for Wikimedia projects](https://dumps.wikimedia.org/other/pagecounts-raw/)

### Wikipedia Articles

L'export complet de Wikipedia version "en".

## Cluster

     export DOJO_HOME=/home/dojo/workspace/coding-dojo-spark/dojo-spark
     export DATA_HOME=/home/dojo/workspace/coding-dojo-spark/data
     export SPARK_HOME=/home/dojo/workspace/spark-1.0.0-bin-hadoop2

### Démarrer le cluster

Attention, il faut que le master et les workers soient déclarés par leur adresses IP plutôt que leurs hostnames. En effet, puisque les hostnames ne sont pas enregistrés dans un DNS, les noms ne peuvent pas être résolus.

Les slaves doivent être accessibles en SSH avec une clé partagée.

Cluster :

* Master : 192.168.1.10
* Slave 1 : 192.168.1.11
* Slave 2 : 192.168.1.12

Démarrer le master :

    export SPARK_MASTER_IP=192.168.1.10
    cd $SPARK_HOME/sbin/start-master.sh

Interface de supervision : http://192.168.1.10:8080

Sur le master, éditer le fichier $SPARK_HOME/conf/slaves :

    192.168.1.10
    192.168.1.11
    192.168.1.12

Démarrer les slaves :

    $SPARK_HOME/sbin/start_slaves.sh

### Soumettre des jobs

Préparer l'exécuteur :

    SparkConf conf = new SparkConf()
            .setAppName("...")
            .setMaster("spark://192.168.1.10:7077")
            .setJars(new String[]{PATH + "/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar"});

Préparer le JAR avec Maven :

    mvn package

Distribuer le JAR sur les slaves :

    scp $DOJO_HOME/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar 192.168.1.11:$DOJO_HOME/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar
    scp $DOJO_HOME/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar 192.168.1.12:$DOJO_HOME/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar

Soumettre un job (ici AnalyseWikipediaWorldCup) :

    $SPARK_HOME/bin/spark-submit --class fr.ippon.dojo.spark.AnalyseWikipediaWorldCup --master spark://192.168.1.10:7077 --deploy-mode cluster $DOJO_HOME/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar
