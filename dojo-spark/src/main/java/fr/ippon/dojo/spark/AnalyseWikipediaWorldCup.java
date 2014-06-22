package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AnalyseWikipediaWorldCup {
    public static void main(String[] args) {

        final String PATH = "/Users/seigneurin/dev/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("wikipedia-pagecounts worldcup")
                .setMaster("local");
        //.setMaster("spark://192.168.1.48:7077")
        //.setJars(new String[]{PATH + "/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        String worldcupPages = PATH + "/data/wikipedia-worldcup-pages/worldcup-pages.txt";
        JavaPairRDD<String, String> worldcupPagesRDD = sc.textFile(worldcupPages, 10)
                .map(line -> line.split("\t"))
                .mapToPair(s -> new Tuple2<String, String>(s[0] + " " + s[3].toLowerCase(), s[1] + " - " + s[2]));

        // DEBUG
        if (!true) {
            worldcupPagesRDD.foreach(s -> System.out.println(s._1));
        }

        String worldcupViews = PATH + "/data/wikipedia-pagecounts-days/pagecounts-*";
        JavaPairRDD<String, Long> worldcupViewsRDD = sc.textFile(worldcupViews)
                .map(line -> line.split(" "))
                .mapToPair(s -> new Tuple2<String, Long>(s[0] + " " + s[1].toLowerCase(), Long.parseLong(s[2])))
                .reduceByKey((x, y) -> x + y, 10);

        // DEBUG
        if (!true) {
            worldcupViewsRDD.foreach(s -> System.out.println(s._1));
        }

        JavaPairRDD<String, scala.Tuple2<Long, String>> worldcupPageViewsRDD = worldcupViewsRDD
                .join(worldcupPagesRDD)
                .cache();

        worldcupPageViewsRDD
                .mapToPair(x -> new Tuple2<Long, String>(x._2._1, x._2._2))
                .sortByKey()
                .saveAsTextFile(PATH + "/data/res-worldcup-pageviews-byviews");

        worldcupPageViewsRDD
                .mapToPair(x -> new Tuple2<String, Long>(x._2._2, x._2._1))
                .sortByKey()
                .saveAsTextFile(PATH + "/data/res-worldcup-pageviews-bycountry");
    }
}
