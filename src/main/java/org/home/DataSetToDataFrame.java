package org.home;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DataSetToDataFrame {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Array to Dataset<String>").master("local").getOrCreate();

        String[] stringList = new String[]{"banana", "orange", "banana", "carrot", "mango", "mango"};
        System.out.println(stringList);

        List<String> data = Arrays.asList(stringList);
        System.out.println(data);

        Dataset<String> ds = sparkSession.createDataset(data, Encoders.STRING());
        ds.printSchema();
        ds.show();
        Dataset<Row> df = ds.groupBy("value").count();
        df.printSchema();
        df.show();

        //convert dataset to dataframe using .toDF();
        Dataset<Row> df1 = sparkSession.createDataset(data, Encoders.STRING()).toDF();
        //convert dataframe to dataset
        ds = df1.as(Encoders.STRING());
        System.out.println("Converted DF to DS: ");
        ds.printSchema();
        ds.show();

        df1 = df1.groupBy(df1.col("value")).count();
        df1.printSchema();
        df1.show();

        String[] fruit = {"orange", "grapes"};
        List<String> fruitList = Arrays.asList(fruit);
        Dataset<Row> df2 = sparkSession.createDataset(fruitList, Encoders.STRING()).toDF().groupBy("value").count();
        df2.printSchema();
        df2.show();

        Dataset<Row> df3 = df2.intersect(df1);
        df3.printSchema();
        df3.show();
    }
}
