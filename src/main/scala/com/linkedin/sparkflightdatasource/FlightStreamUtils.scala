package com.linkedin.sparkflightdatasource

import org.apache.arrow.flight.FlightStream
import org.apache.spark.sql.vectorized.ColumnarBatch

import collection.JavaConverters._

object FlightStreamUtils {

  var totalRowCount: Int = 0

  def streamNexts(streams: java.util.List[FlightStream]) = {
    streams.asScala.forall(_.next())
  }

  def streamsToFieldVectors(streams: java.util.List[FlightStream]) = {
    //val batch = new ColumnarBatch(
      streams
        .asScala
        //.par
        .flatMap(stream => {
          val root = stream
            .getRoot
            val vectors = root
            .getFieldVectors
            .asScala
          totalRowCount += root.getRowCount
          root.close()
          stream.close()
          vectors
      //      .map(fv => new FlightArrowColumnVector(fv))
        })
        .toArray
    //)
  }

  def getTotalRowCount(streams: java.util.List[FlightStream]) = {
    totalRowCount
  }

}
