package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class FirstProgram {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        // Create a session
        SparkSession sparkSession = SparkSession.builder().appName("CSV TO DB").master("local").getOrCreate();
        // Get data
        Dataset<Row> df = sparkSession.read().format("csv").option("header", true).load("src/main/resources/name_and_comments.txt");
        df.show(false);

        // Transformation
        df = df.withColumn("full name", concat(df.col("last_name").rlike("\\d+"), lit(", "), df.col("first_name")));
        df.show(false);

        df = df.filter(df.col("comment").rlike("\\d+"));
        df.show(false);

        df = df.sort(df.col("last_name").asc());
        df.show(false);

        String dbConnection = "jdbc:postgresql://localhost/learnSpark_db";
        Properties properties = new Properties();
        properties.setProperty("driver", "org.postgresql.Driver");
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "admin123");

        df.write().mode(SaveMode.Overwrite).jdbc(dbConnection, "table1", properties);

    }
}