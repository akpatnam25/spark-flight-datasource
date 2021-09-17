package com.linkedin.sparkflightdatasource.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;


public class FlightSourceWriterBuilder implements WriteBuilder {

  private LogicalWriteInfo logicalWriteInfo;

  public FlightSourceWriterBuilder(LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new FlightSourceBatchWrite(this.logicalWriteInfo);
  }

}
