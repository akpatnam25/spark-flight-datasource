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
package com.linkedin.sparkflightdatasource;

import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


public class FlightSourceTable implements SupportsRead, SupportsWrite {

  private final StructType schema;
  private FlightSourceParams params;

  public FlightSourceTable(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new FlightSourceScanBuilder(schema, params);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return null;
  }

  @Override
  public String name() {
    return "SparkArrowFlightSource";
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<TableCapability>();
    capabilities.add(TableCapability.BATCH_READ);
    capabilities.add(TableCapability.BATCH_WRITE);
    return capabilities;
  }
}
