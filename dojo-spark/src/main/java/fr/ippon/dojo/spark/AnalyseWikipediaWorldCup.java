package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AnalyseWikipediaWorldCup {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("wikipedia-pagecounts worldcup")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String worldcupPages = "../data/wikipedia-worldcup-pages/worldcup-pages.txt";
        JavaPairRDD<String, String> worldcupPagesRDD = sc.textFile(worldcupPages)
                .map(line -> line.split("\t"))
                .mapToPair(s -> new Tuple2<String, String>(s[0] + " " + s[3].toLowerCase(), s[1] + " - " + s[2]));

        // DEBUG
        if (!true) {
            worldcupPagesRDD.foreach(s -> System.out.println(s._1));
        }

        String worldcupViews = "../data/wikipedia-pagecounts-days/pagecounts-*";
        JavaPairRDD<String, Long> worldcupViewsRDD = sc.textFile(worldcupViews)
                .map(line -> line.split(" "))
                .mapToPair(s -> new Tuple2<String, Long>(s[0] + " " + s[1].toLowerCase(), Long.parseLong(s[2])))
                .reduceByKey((x, y) -> x + y);

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
                .saveAsTextFile("../data/res-worldcup-pageviews-byviews");

        worldcupPageViewsRDD
                .mapToPair(x -> new Tuple2<String, Long>(x._2._2, x._2._1))
                .sortByKey()
                .saveAsTextFile("../data/res-worldcup-pageviews-bycountry");

        /*worldcupPageViewsRDD
                .sortByKey()
                .foreach(s -> System.out.println(s._2._2 + " = " + s._2._1));*/
    }
}
