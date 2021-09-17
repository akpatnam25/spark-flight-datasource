package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightSourceParams;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;


public class FlightSourceDataWriterFactory implements DataWriterFactory {

  private StructType schema;
  private FlightSourceParams params;
  private Map<Integer, String> partitionArrowFlightServerMap;

  public FlightSourceDataWriterFactory(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
    this.partitionArrowFlightServerMap = createPartitionArrowFlightServerMapping();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    String arrowFlightServer = partitionToArrowFlightServer(partitionId);
    return new FlightSourceDataWriter(partitionId, taskId, schema, params, arrowFlightServer);
  }

  private String partitionToArrowFlightServer(int partitionId) {
    return partitionArrowFlightServerMap.get(partitionId); // should be getOrDefault later
  }

  private Map<Integer, String> createPartitionArrowFlightServerMapping() {
    List<String> arrowFlightServers = params.getLocalityInfo();
    long numPartitions = params.getNumPartitions();
    if ((long) arrowFlightServers.size() == numPartitions) {
      System.out.println("Arrow Flight Servers and Partitions will have 1:1 mapping.");
      List<Integer> partitions = new ArrayList<Integer>();
      for (int i = 0; i < numPartitions; i++) {
        partitions.add(i);
      }
      return IntStream.range(0, arrowFlightServers.size())
          .boxed()
          .collect(Collectors.toMap(partitions::get, arrowFlightServers::get));
    } else {
      // Unimplemented
      return new HashMap<Integer, String>();
    }
  }
}
