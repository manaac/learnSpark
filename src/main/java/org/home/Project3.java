package org.home;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Project3 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Combine 2 Datasets").master("local").getOrCreate();

        Dataset<Row> durhamDF = buildDurhamParksDataFrame(sparkSession);
        System.out.println("buildDurhamParksDataFrame schema:");
        durhamDF.printSchema();
        System.out.println("buildDurhamParksDataFrame output:");
        durhamDF.show(10, false);

        Dataset<Row> philDF = buildPhilParksDataFrame(sparkSession);
        System.out.println("buildPhilParksDataFrame schema:");
        philDF.printSchema();
        System.out.println("buildPhilParksDataFrame output:");
        philDF.show(10, false);

        combineDataFrames(durhamDF, philDF);
    }

    private static void combineDataFrames(Dataset<Row> durhamDF, Dataset<Row> philDF) {
        // Match by column names using the unionByName() method.
        // if we use just the union() method, it matches the columns based on order.
        Dataset<Row> df = durhamDF.unionByName(philDF);
        System.out.println("combineDataFrames schema:");
        df.printSchema();
        System.out.println("combineDataFrames output:");
        df.show(300);
        System.out.println("the total number of records are: " + df.count());

        Partition[] partitions = df.rdd().partitions();
        System.out.println("Total number of partitions are: " + partitions.length);

        df = df.repartition(5);
        Partition[] partitions1 = df.rdd().partitions();
        System.out.println("Total number of partitions after repartition are: " + partitions1.length);
    }

    private static Dataset<Row> buildPhilParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().format("CSV")
                .option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");
        System.out.println("source PhilParks schema:");
        df.printSchema();
        System.out.println("source PhilParks output:");
        df.show(10, 100);

//        df = df.filter(lower(df.col("USE_")).like("%park%"));
        df = df.filter("lower(USE_) like '%park%'")
                .withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("city", lit("Philadelphia"))
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acre")
                .withColumn("geoX", lit("UNKNOWN"))
                .withColumn("geoY", lit("UNKNOWN"))
                .drop("OBJECTID")
                .drop("SITE_NAME").drop("CHILD_OF")
                .drop("TYPE").drop("USE_").drop("DESCRIPTION").drop("SQ_FEET")
                .drop("ALLIAS").drop("CHRONOLOGY").drop("NOTES")
                .drop("DATE_EDITED").drop("EDITED_BY").drop("OCCUPANT")
                .drop("TENANT").drop("LABEL");
        return df;
    }

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().format("JSON")
                .option("multiline", true)
                .load("src/main/resources/durham-parks.json");
        System.out.println("source DurhamParks schema:");
        df.printSchema();
        System.out.println("source DurhamParks output:");
        df.show(10, 100);
        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("-"), df.col("fields.objectid")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("address", df.col("fields.address"))
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
