package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AnalyseWikipediaWorldCup {
    public static void main(String[] args) {

        final String PATH = "/home/dojo/workspace/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("wikipedia-pagecounts worldcup")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String worldcupPages = PATH + "/data/wikipedia-worldcup-pages/worldcup-pages.txt";
        sc.textFile(worldcupPages, 10);
        //...

        String worldcupViews = PATH + "/data/wikipedia-pagecounts-days/pagecounts-*";
        sc.textFile(worldcupViews);
        // ...
    }
}
