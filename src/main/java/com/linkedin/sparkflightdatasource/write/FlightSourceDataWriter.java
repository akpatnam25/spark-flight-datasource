package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightArrowUtils;
import com.linkedin.sparkflightdatasource.FlightSourceParams;
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
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.arrow.vector.types.pojo.Schema;


public class FlightSourceDataWriter implements DataWriter<InternalRow> {

  private int partitionId;
  private long taskId;
  private FlightSourceParams params;
  //private StructType schema;
  //private String arrowFlightServer;

  private ArrowWriter arrowWriter;
  private FlightClient client;
  private FlightClient.ClientStreamListener stream;
  //private VectorSchemaRoot root;
  //private FlightDescriptor descriptor;

  public FlightSourceDataWriter(int partitionId, long taskId,StructType schema, FlightSourceParams params, String arrowFlightServer) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.params = params;
    //this.schema = schema;
    //this.arrowFlightServer = arrowFlightServer;

    String[] hostPort = arrowFlightServer.split(":");

    Schema arrowSchema = ArrowUtils.toArrowSchema(schema, null);
    BufferAllocator allocator = FlightArrowUtils.rootAllocator().newChildAllocator("ChildAllocator--" + partitionId, 0, Long.MAX_VALUE);
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
    this.arrowWriter = ArrowWriter.create(root);

    this.client = FlightClient.builder(allocator, Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]))).build();
    FlightDescriptor descriptor = FlightDescriptor.path(params.getDescriptor());

    this.stream = this.client.startPut(descriptor, root, new AsyncPutListener());

  }

  @Override
  public void write(InternalRow row) throws IOException {
    arrowWriter.write(row);
//    if (arrowWriter.root().getRowCount() % 10 == 0) {
//      arrowWriter.finish();
//      while (!stream.isReady()) {
//
//      }
//      stream.putNext();
//      stream.getResult();
//      arrowWriter.root().clear();
//      arrowWriter.reset();
//    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    arrowWriter.finish();

    //System.out.println("FieldVectors: " + root.getFieldVectors());
    //System.out.println("RootRowCount: " + root.getRowCount());

    stream.putNext();
    stream.completed();
    stream.getResult();


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
