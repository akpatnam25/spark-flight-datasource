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
package com.linkedin.sparkflightdatasource.read;

import com.linkedin.sparkflightdatasource.FlightSourceParams;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;


public class FlightSourceBatch implements Batch {
  private final StructType schema;
  private FlightSourceParams params;

  public FlightSourceBatch(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    InputPartition[] partitions = new FlightSourceInputPartition[params.getLocalityInfo().size()];
    for (int i = 0; i < partitions.length; i++) {
      partitions[i] = new FlightSourceInputPartition(params.getLocalityInfo().get(i));
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
   return new FlightSourceInputPartitionReaderFactory(schema, params);
  }
}
