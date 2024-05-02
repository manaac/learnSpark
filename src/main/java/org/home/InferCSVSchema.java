package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", "true")
                .load("src/main/resources/amazonProducts.txt");
        df.show(false);
        df.printSchema();
    }
}

