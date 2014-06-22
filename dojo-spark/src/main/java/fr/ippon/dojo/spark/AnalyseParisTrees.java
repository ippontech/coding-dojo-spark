package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AnalyseParisTrees {
    public static void main(String[] args) {

        final String PATH = "/Users/seigneurin/dev/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("paris-arbresalignementparis2010")
                .setMaster("local");
        //.setMaster("spark://192.168.1.48:7077")
        //.setJars(new String[]{PATH + "/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = PATH + "/data/paris-arbresalignementparis2010/arbresalignementparis2010.csv";
        sc.textFile(filename)
                .filter(s -> s.startsWith("geom_x_y") == false)
                .map(line -> line.split(";"))
                .filter(x -> x[3].equals("") == false)
                .mapToPair(s -> new Tuple2<String, Long>(s[3], 1l))
                .reduceByKey((x, y) -> x + y)
                .foreach(x -> System.out.println(x._1 + " = " + x._2));
    }
}
