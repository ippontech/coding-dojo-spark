package fr.ippon.dojo.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

public class AnalyseParisTreesSQL {
    public static void main(String[] args) {

        // geom_x_y;geom;genre_lati;genre_fran;variete;arbre_rema;circonfere;hauteur_m;date_mesur;lib_type_e;lib_etat_c;x;y

        final String PATH = "/home/dojo/workspace/coding-dojo-spark/";

        SparkConf conf = new SparkConf()
                .setAppName("paris-arbresalignementparis2010")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaSQLContext sqlContext = new JavaSQLContext(sc);

        String filename = PATH + "/data/paris-arbresalignementparis2010/arbresalignementparis2010.csv";
        JavaRDD<Row> rdd = sc.textFile(filename)
                .filter(line -> !line.startsWith("geom"))
                .map(line -> line.split(";", -1))
                .map(fields -> Row.create(fields[0]/*, ...*/));

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataType.createStructField("geom_x_y", DataType.StringType, false));
        // ...
        StructType schema = DataType.createStructType(fields);
        JavaSchemaRDD schemaRDD = sqlContext.applySchema(rdd, schema);

        schemaRDD.registerTempTable("tree");

        long count = sqlContext.sql("SELECT count(*) FROM tree").take(1).get(0).getLong(0);
        System.out.println(count);

    }
}
