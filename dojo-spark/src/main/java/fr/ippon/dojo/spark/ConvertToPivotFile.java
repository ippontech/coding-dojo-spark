package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ConvertToPivotFile {

    public static void main(String[] args) {

        final String PATH = "/Users/seigneurin/dev/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("paris-tonnagesdechets-topivotfile")
                .setMaster("local");
        //.setMaster("spark://192.168.1.48:7077")
        //.setJars(new String[]{PATH + "/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = PATH + "/data/paris-tonnagesdechets-flat/part-*";
        sc.textFile(filename)
                .map(line -> line.split(";"))
                .mapToPair(t -> new Tuple2<>(t[0], mapToPivotLine(t)))
                .reduceByKey((x, y) -> reduce(x, y))
                .map(t -> t._1 + ";" + String.join(";", t._2))
                //.foreach(s -> System.out.println(s));
                .saveAsTextFile(PATH + "/data/paris-tonnagesdechets-pivot");
    }

    private static String[] reduce(String[] x, String[] y) {
        for (int i = 0; i < x.length; i++) {
            if (y[i] != null)
                x[i] = y[i];
        }
        return x;
    }

    private static String[] mapToPivotLine(String[] t) {
        int month = monthToIndex(t[1]) - 1;
        String[] res = new String[12];
        res[month] = t[2];
        return res;
    }

    private static int monthToIndex(String month) {
        switch (month) {
            case "janv_11":
                return 1;
            case "fevr_11":
                return 2;
            case "mars_11":
                return 3;
            case "avr_11":
                return 4;
            case "mai_11":
                return 5;
            case "juin_11":
                return 6;
            case "juil_11":
                return 7;
            case "aout_11":
                return 8;
            case "sept_11":
                return 9;
            case "oct_11":
                return 10;
            case "nov_11":
                return 11;
            case "dec_11":
                return 12;
        }
        throw new RuntimeException("Unknown: " + month);
    }
}
