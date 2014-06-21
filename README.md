# Coding Dojo - Apache Spark & Open Data


## Ressources

* [Spark](http://spark.apache.org/)
* [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html)
* [Making Apache Spark Easier to Use in Java with Java 8](http://blog.cloudera.com/blog/2014/04/making-apache-spark-easier-to-use-in-java-with-java-8/)


## Wikipedia Pagecounts

Source : [Page view statistics for Wikimedia projects](https://dumps.wikimedia.org/other/pagecounts-raw/)

    mvn exec:java -Dexec.mainClass=fr.ippon.dojo.spark.AnalyseWikipediaWorldCup

## Cluster

Attention, il faut que le master et les workers soient déclarés par leur adresses IP plutôt que leurs hostnames. En effet, puisque les hostnames ne sont pas enregistrés dans un DNS, les noms ne peuvent pas être résolus.

Démarrer le master :

    export SPARK_MASTER_IP=192.168.1.xxx
    cd $SPARK_HOME
    ./sbin/start-master.sh

Interface de supervision : http://192.168.1.xxx:8080

Démarrer un worker :

    /bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.xxx:7077 -i 192.168.1.yyy
