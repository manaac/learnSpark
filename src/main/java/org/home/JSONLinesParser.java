package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JSON").master("local").getOrCreate();

        Dataset<Row> df = sparkSession.read().format("json").load("src/main/resources/simple.json");
        df.show(false);
        df.printSchema();

        Dataset<Row> df1 = sparkSession.read().format("json").option("multiline", true).load("src/main/resources/multiline.json");
        df1.show(false);
        df1.printSchema();
    }
}
