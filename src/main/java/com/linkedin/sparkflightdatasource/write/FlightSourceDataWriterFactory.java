package com.linkedin.sparkflightdatasource.write;

import com.linkedin.sparkflightdatasource.FlightSourceParams;
import com.linkedin.sparkflightdatasource.SchemaStatic;
import java.util.Random;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;


public class FlightSourceDataWriterFactory implements DataWriterFactory {

  private StructType schema;
  private FlightSourceParams params;
  // private Map<Integer, String> partitionArrowFlightServerMap;
  // private BufferAllocator rootAllocator;

  public FlightSourceDataWriterFactory(StructType schema, FlightSourceParams params) {
    this.schema = schema;
    this.params = params;
    // this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
    //this.partitionArrowFlightServerMap = createPartitionArrowFlightServerMapping();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    String arrowFlightServer = partitionToArrowFlightServer(partitionId);
    System.out.println(arrowFlightServer + "---" + partitionId);
    SchemaStatic.schema = this.schema;
    return new FlightSourceDataWriter(partitionId, taskId, schema, params, arrowFlightServer);
  }

  private String partitionToArrowFlightServer(int partitionId) {
    int numPartitions = (int) params.getNumPartitions();
    int numHosts = params.getLocalityInfo().size();
    int scaleFactor = numPartitions / numHosts;

    int hostIndex = partitionId / scaleFactor;
    if (hostIndex >= 0 && hostIndex <= 4) {
      return params.getLocalityInfo().get(hostIndex);
    } else {
      Random r = new Random();
      int randHostIndex = r.nextInt(numHosts);
      return params.getLocalityInfo().get(randHostIndex);
    }

    // return partitionArrowFlightServerMap.get(partitionId); // should be getOrDefault later
  }

//  private Map<Integer, String> createPartitionArrowFlightServerMapping() {
//    List<String> arrowFlightServers = params.getLocalityInfo();
//    long numPartitions = params.getNumPartitions();
//    if ((long) arrowFlightServers.size() == numPartitions) {
//      System.out.println("Arrow Flight Servers and Partitions will have 1:1 mapping.");
//      List<Integer> partitions = new ArrayList<Integer>();
//      for (int i = 0; i < numPartitions; i++) {
//        partitions.add(i);
//      }
//      return IntStream.range(0, arrowFlightServers.size())
//          .boxed()
//          .collect(Collectors.toMap(partitions::get, arrowFlightServers::get));
//    } else {
//      // Unimplemented
//      return new HashMap<Integer, String>();
//    }
//  }
}
