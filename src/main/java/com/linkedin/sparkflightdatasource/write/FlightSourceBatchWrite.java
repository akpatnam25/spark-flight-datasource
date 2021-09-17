package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightSourceParams;
import com.linkedin.sparkflightdatasource.Util;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;


public class FlightSourceBatchWrite implements BatchWrite {

  private LogicalWriteInfo logicalWriteInfo;
  private FlightSourceParams params;

  public FlightSourceBatchWrite(LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
    this.params = Util.extractOptions(logicalWriteInfo.options());
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new FlightSourceDataWriterFactory(logicalWriteInfo.schema(), params);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {

  }

  @Override
  public void abort(WriterCommitMessage[] messages) {

  }

  public boolean useCommitCoordinator() {
    return true;
  }
}
