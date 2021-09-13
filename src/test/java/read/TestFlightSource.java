/*
 * Copyright (C) 2019 Ryan Murray
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package read;

import com.linkedin.sparkflightdatasource.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TestFlightSource {

  public static void main(String [] args) {
    SparkSession sparkSession = SparkSession.builder()
        .appName("data_source_test")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> dataset = sparkSession.read()
        .format("com.linkedin.sparkflightdatasource.FlightSource")
        .option(Constants.DESCRIPTOR, "spark-flight-descriptor")
        .option(Constants.LOCALITY_INFO, "localhost:9002,localhost:9004")
        .option(Constants.PARTITIONING_COLUMN, "doesnt_matter")
        .load();
    dataset.show();;
    System.out.println(dataset.count());
    System.out.println(dataset.rdd().getNumPartitions());
  }


}

