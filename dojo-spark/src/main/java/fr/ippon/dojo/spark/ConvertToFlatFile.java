package fr.ippon.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class ConvertToFlatFile {

    public static void main(String[] args) {

        final String PATH = "/Users/seigneurin/dev/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("paris-tonnagesdechets-toflatfile")
                .setMaster("local");
        //.setMaster("spark://192.168.1.48:7077")
        //.setJars(new String[]{PATH + "/dojo-spark/target/dojo-spark-0.0.1-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename = PATH + "/data/paris-tonnagesdechets/tonnages_des_dechets_bacs_jaunes.csv";
        String[] headers = sc.textFile(filename)
                .map(line -> line.split(";"))
                .first();

        sc.textFile(filename)
                .map(line -> line.split(";"))
                .filter(t -> !t[0].equals("Tout Paris") && !t[0].equals("granularite"))
                .flatMap(t -> flattenize(t, headers))
                .map(t -> t[0] + ";" + t[1] + ";" + t[2])
                .saveAsTextFile(PATH + "/data/paris-tonnagesdechets-flat");
    }

    private static Iterable<String[]> flattenize(String[] tokens, String[] headers) {
        ArrayList<String[]> res = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            res.add(new String[]{tokens[0], headers[i + 1], tokens[i + 1]});
        }
        return res;
    }
}
