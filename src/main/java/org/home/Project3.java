package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Project3 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Combine 2 Datasets").master("local").getOrCreate();

        Dataset<Row> durhamDF = buildDurhamParksDataFrame(sparkSession);
        durhamDF.printSchema();
        durhamDF.show(10, false);
    }

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().format("JSON")
                .option("multiline", true)
                .load("src/main/resources/durham-parks.json");
        df.printSchema();
        df.show(10, 100);
        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("-"), df.col("fields.objectid")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acre", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("datasetid").drop("fields").drop("geometry")
                .drop("record_timestamp").drop("recordid");

        return df;
    }
}
