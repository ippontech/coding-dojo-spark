package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AnalyseParisTrees {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("paris-arbresalignementparis2010")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String worldcupPages = "../data/paris-arbresalignementparis2010/arbresalignementparis2010.csv";
        sc.textFile(worldcupPages)
                .filter(s -> s.startsWith("geom_x_y") == false)
                .map(line -> line.split(";"))
                .filter(x -> x[3].equals("") == false)
                .mapToPair(s -> new Tuple2<String, Long>(s[3], 1l))
                .reduceByKey((x, y) -> x + y)
                .sortByKey()
                .foreach(x -> System.out.println(x._1 + " = " + x._2));
    }
}
