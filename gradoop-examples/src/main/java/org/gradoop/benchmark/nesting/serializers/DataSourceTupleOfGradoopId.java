package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DataSourceTupleOfGradoopId extends FileInputFormat<Tuple2<GradoopId,GradoopId>> {

  /**
   * Reusable array
   */
  private final byte[] bytes;

  /**
   * Reusable Pair
   */
  private final Tuple2<GradoopId,GradoopId> reusable;

  /**
   * Default constructor
   */
  public DataSourceTupleOfGradoopId() {
    bytes = new byte[GradoopId.ID_SIZE];
    reusable = new Tuple2<>();
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return stream.available()>0;
  }

  @Override
  public Tuple2<GradoopId,GradoopId> nextRecord(Tuple2<GradoopId,GradoopId> gradoopId) throws IOException {
    stream.read(bytes);
    reusable.f0 = GradoopId.fromByteArray(bytes);
    stream.read(bytes);
    reusable.f1 = GradoopId.fromByteArray(bytes);
    return reusable;
  }

}
