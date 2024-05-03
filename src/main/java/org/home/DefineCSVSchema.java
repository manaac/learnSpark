package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("item_name", DataTypes.StringType, true),
                DataTypes.createStructField("published_on", DataTypes.DateType, true),
                DataTypes.createStructField("url", DataTypes.StringType, true)
        });

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("multiline", true)
                .option("dateFormat", "M/d/y")
                .schema(schema)
                .load("src/main/resources/amazonProducts.txt");

        df.show(false);
        df.printSchema();
    }
}
