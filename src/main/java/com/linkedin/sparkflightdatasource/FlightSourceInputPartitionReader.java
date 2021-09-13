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

import java.io.IOException;
import java.util.List;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;


public class FlightSourceInputPartitionReader implements PartitionReader<ColumnarBatch> {

  private final FlightSourceInputPartition inputPartition;
  private final FlightSourceParams params;
  private final StructType schema;

  private FlightClient client;
  private FlightStream stream;
  private BufferAllocator allocator;
  private FlightDescriptor flightDescriptor;
  private FlightInfo flightInfo;
  private Location location;
  private Ticket ticket;

  public FlightSourceInputPartitionReader(FlightSourceInputPartition inputPartition, StructType schema, FlightSourceParams params) {
    this.inputPartition = inputPartition;
    this.schema = schema;
    this.params = params;

    start();
  }

  private void start() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    String[] hostPort = inputPartition.preferredLocations()[0].split(":");
    this.location = Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]));
    this.client = FlightClient
        .builder()
        .allocator(allocator)
        .location(location)
        .build();
    this.flightDescriptor = FlightDescriptor.path(params.getDescriptor());
    this.flightInfo = client.getInfo(flightDescriptor);
    System.out.println("FlightInfo get endpoints size: " + flightInfo.getEndpoints().size());
    FlightEndpoint endpoint = flightInfo.getEndpoints().get(0);
    this.ticket = endpoint.getTicket();
    this.stream = client.getStream(ticket);
  }

  @Override
  public boolean next() throws IOException {
    try {
      return stream.next();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public ColumnarBatch get() {
    ColumnarBatch batch = new ColumnarBatch(
        stream.getRoot().getFieldVectors()
            .stream()
            .map(FlightArrowColumnVector::new)
            .toArray(ColumnVector[]::new)
    );
    batch.setNumRows(stream.getRoot().getRowCount());
    return batch;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(stream, client, allocator);
      allocator.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
