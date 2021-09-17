package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.Constants;
import com.linkedin.sparkflightdatasource.FlightSource;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


public class TestFlightSourceWrite {


  public static void main(String [] args) {
    SparkSession spark = SparkSession.builder()
        .appName("data_source_test")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> dataset =
        spark
        .read()
        .csv("/Users/apatnam/dev/livy-test-data/")
        .toDF("age", "first", "last", "company")
        .repartition(2);
    System.out.println("Count: " + dataset.count());

    dataset
        .write()
        .format(FlightSource.class.getCanonicalName())
        .option(Constants.DESCRIPTOR, "spark-flight-descriptor")
        .option(Constants.LOCALITY_INFO, "localhost:9002,localhost:9004")
        .option(Constants.PARTITIONING_COLUMN, "doesnt_matter")
        .option(Constants.NUM_PARTITIONS, String.valueOf(dataset.rdd().getNumPartitions()))
        .mode(SaveMode.Append)
        .save();
  }

}
