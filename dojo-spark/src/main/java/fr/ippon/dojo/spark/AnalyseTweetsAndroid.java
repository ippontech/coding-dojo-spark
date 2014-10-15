package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.TwitterException;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.*;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class AnalyseTweetsAndroid {
    public static void main(String[] args) throws TwitterException, InterruptedException {

        String pathFile = "hashtags.txt";

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        SparkConf sparkConf = new SparkConf()
                .setAppName("tweets worldcup")
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        String[] filters = {"#Android"};

        // Filtres pour supprimer le HashTags #Android (et #android)
        List<String> hashtagFilter = Arrays.asList("android", "Android");
        
        JavaPairDStream<String, Integer> hashtags = TwitterUtils.createStream(sc, twitterAuth, filters)
                .flatMap(s -> Arrays.asList(s.getHashtagEntities()))
                .mapToPair(h -> new Tuple2<>(h.getText(), 1));

        // Toutes les 10 secondes, on calcule ce qui s'est passé sur les 30 dernières secondes
        hashtags.reduceByKeyAndWindow((i1, i2) -> i1 + i2, new Duration(30000), new Duration(10000))
                .filter(ha -> !hashtagFilter.contains(ha._1))
                .foreach(rdd -> {
                    String date = "Time : " + DateFormat.getDateTimeInstance().format(Calendar.getInstance().getTime());
                    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(pathFile, true)))) {
                        out.println(date);
                    } catch (IOException e) {
                        //exception handling left as an exercise for the reader
                    }

                    rdd.mapToPair(Tuple2::swap)
                            .sortByKey(false)
                            .foreach(h1 -> {
                                try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(pathFile, true)))) {
                                    out.println(h1);
                                } catch (IOException e) {
                                    //exception handling left as an exercise for the reader
                                }
                            });
                    return null;
                });

        sc.start();
        sc.awaitTermination();
    }
}
