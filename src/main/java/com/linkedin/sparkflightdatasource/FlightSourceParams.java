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

import java.io.Serializable;
import java.util.List;


public class FlightSourceParams implements Serializable {
  private final FlightSourceParamsBuilder builder;
  private List<String> localityInfo;
  private String partitioningColumn;
  private String descriptor;
  private long numPartitions;

  public FlightSourceParams(FlightSourceParamsBuilder builder) {
    this.localityInfo = builder.localityInfo;
    this.partitioningColumn = builder.partitioningColumn;
    this.descriptor = builder.descriptor;
    this.numPartitions = builder.numPartitions;
    this.builder = builder;
  }

  public List<String> getLocalityInfo() {
    return localityInfo;
  }

  public String getPartitioningColumn() {
    return partitioningColumn;
  }

  public String getDescriptor() {
    return descriptor;
  }

  public long getNumPartitions() { return numPartitions; }

  public static class FlightSourceParamsBuilder implements Serializable {
    private List<String> localityInfo;
    private String partitioningColumn;
    private String descriptor;
    private long numPartitions;

    public FlightSourceParamsBuilder setLocalityInfo(List<String> localityInfo) {
      this.localityInfo = localityInfo;
      return this;
    }

    public FlightSourceParamsBuilder setPartitioningColumn(String partitioningColumn) {
      this.partitioningColumn = partitioningColumn;
      return this;
    }

    public FlightSourceParamsBuilder setDescriptor(String descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    public FlightSourceParamsBuilder setNumPartitions(long numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public FlightSourceParams build() {
      return new FlightSourceParams(this);
    }
  }



}
