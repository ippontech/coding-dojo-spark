package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.TwitterException;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class AnalyseTweetsWorldCup {
    public static void main(String[] args) throws TwitterException, InterruptedException {

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        SparkConf sparkConf = new SparkConf()
                .setAppName("tweets worldcup")
                .setMaster("local");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        String[] filters = {"#WorldCup"};
        TwitterUtils.createStream(sc, twitterAuth, filters)
                .map(s -> s.getText())
                .print();

        sc.start();
        sc.awaitTermination();
    }
}
