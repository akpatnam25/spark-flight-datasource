package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightSourceParams;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;


public class FlightSourceDataWriterFactory implements DataWriterFactory {

  private StructType schema;
  private FlightSourceParams params;
  //private Map<Integer, String> partitionArrowFlightServerMap;

  public FlightSourceDataWriterFactory(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
    //this.partitionArrowFlightServerMap = createPartitionArrowFlightServerMapping();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    String arrowFlightServer = partitionToArrowFlightServer(partitionId);
    System.out.println(arrowFlightServer + "---" + partitionId);
    return new FlightSourceDataWriter(partitionId, taskId, schema, params, arrowFlightServer);
  }

  private String partitionToArrowFlightServer(int partitionId) {
    int numPartitions = (int) params.getNumPartitions();
    int numHosts = params.getLocalityInfo().size();
    if (numHosts == numPartitions) {
      return params.getLocalityInfo().get(partitionId);
    } else {
      int scaleFactor = numPartitions / numHosts;
      int hostIndex = partitionId / scaleFactor;
      if (hostIndex >= 0 && hostIndex <= 4) {
        return params.getLocalityInfo().get(hostIndex);
      } else {
        Random r = new Random();
        int randHostIndex = r.nextInt(numHosts);
        return params.getLocalityInfo().get(randHostIndex);
      }
    }
  }
}
