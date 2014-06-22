package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AnalyseParisTrees {
    public static void main(String[] args) {

        final String PATH = "/home/dojo/workspace/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("paris-arbresalignementparis2010")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = PATH + "/data/paris-arbresalignementparis2010/arbresalignementparis2010.csv";
        sc.textFile(filename)
                .count();
    }
}
