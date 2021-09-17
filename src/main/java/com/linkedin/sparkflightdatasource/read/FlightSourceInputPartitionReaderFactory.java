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
import com.linkedin.sparkflightdatasource.read.FlightSourceInputPartition;
import com.linkedin.sparkflightdatasource.read.FlightSourceInputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;


public class FlightSourceInputPartitionReaderFactory implements PartitionReaderFactory {

  private final StructType schema;
  private final FlightSourceParams params;

  public FlightSourceInputPartitionReaderFactory(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return null;
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    return new FlightSourceInputPartitionReader((FlightSourceInputPartition) partition, schema, params);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
