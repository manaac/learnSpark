package org.home;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class MapReduce {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Map Reduce").master("local").getOrCreate();

        String[] fruit = new String[]{"orange", "mango", "carrot"};
        List<String> fruitList = Arrays.asList(fruit);
        Dataset<String> ds = sparkSession
                .createDataset(fruitList, Encoders.STRING());
        ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
        ds.printSchema();
        ds.show();
    }
}
