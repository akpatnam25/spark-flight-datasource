package com.linkedin.sparkflightdatasource;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;


public class MemoryStatics {


  public static BufferAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  public static VectorSchemaRoot root = VectorSchemaRoot.create(FlightArrowUtils.toArrowSchema(SchemaStatic.schema, null), rootAllocator);


}
