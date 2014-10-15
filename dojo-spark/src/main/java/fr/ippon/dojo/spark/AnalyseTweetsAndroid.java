package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import twitter4j.TwitterException;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class AnalyseTweetsAndroid {
    public static void main(String[] args) throws TwitterException, InterruptedException {

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        SparkConf sparkConf = new SparkConf()
                .setAppName("tweets worldcup")
                .setMaster("local[2]");

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        String[] filters = {"#Android"};
        TwitterUtils.createStream(sc, twitterAuth, filters)
            // ...
    }
}
