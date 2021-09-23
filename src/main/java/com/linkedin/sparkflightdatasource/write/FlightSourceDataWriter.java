package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightArrowUtils;
import com.linkedin.sparkflightdatasource.FlightSourceParams;
import com.linkedin.sparkflightdatasource.MemoryStatics;
import java.io.IOException;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructType;
//import org.apache.spark.sql.util.ArrowUtils;
import org.apache.arrow.vector.types.pojo.Schema;


public class FlightSourceDataWriter implements DataWriter<InternalRow> {

  private int partitionId;
  private long taskId;
  private FlightSourceParams params;
  //private StructType schema;
  private String arrowFlightServer;

  private ArrowWriter arrowWriter;
  private FlightClient client;
  private FlightClient.ClientStreamListener stream;
  //private VectorSchemaRoot root;
  private FlightDescriptor descriptor;

  public FlightSourceDataWriter(int partitionId, long taskId,StructType schema, FlightSourceParams params, String arrowFlightServer) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.params = params;
    //this.schema = schema;
    this.arrowFlightServer = arrowFlightServer;

    String[] hostPort = arrowFlightServer.split(":");

    // Schema arrowSchema = FlightArrowUtils.toArrowSchema(schema, null);
//    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    BufferAllocator childAllocator =
        MemoryStatics.rootAllocator.newChildAllocator("ChildAllocator--" + partitionId, 0, Long.MAX_VALUE);
    //this.root = VectorSchemaRoot.create(arrowSchema, childAllocator);
    VectorSchemaRoot root = MemoryStatics.root;
    this.arrowWriter = ArrowWriter.create(root);

    this.client = FlightClient.builder(childAllocator, Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]))).build();
    this.descriptor = FlightDescriptor.path(params.getDescriptor());

    this.stream = this.client.startPut(this.descriptor, root, new AsyncPutListener());

  }

  @Override
  public void write(InternalRow row) throws IOException {
    arrowWriter.write(row);
//    try {
//      if (row != null && !row.anyNull()) {
//        arrowWriter.write(row);
//      } else {
//        System.out.println("skipping row...because of nulls");
//      }
//    } catch (Exception e) {
//      System.out.println("skipping row..." + e.getMessage());
////      int rowCount = arrowWriter.root().getRowCount();
////      if (rowCount == 1000) {
////        while (!stream.isReady()) {
////
////        }
////        arrowWriter.finish();
////        stream.putNext();
////        arrowWriter.reset();
////      }
//    }

  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    while (!stream.isReady()) {

    }
//    if (stream.isReady()) {
    arrowWriter.finish();

//    System.out.println("FieldVectors: " + root.getFieldVectors());
//    System.out.println("RootRowCount: " + root.getRowCount());


    stream.putNext();
    stream.completed();
    stream.getResult();
//    }
    return new WriterCommitMessageImpl(partitionId, taskId);
  }

  @Override
  public void abort() throws IOException {
    System.out.println("in abort...failing");
  }

  @Override
  public void close() throws IOException {
    try {
      client.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void putRecordBatch() {

  }
}
