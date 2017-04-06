package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DataSourceGradoopId extends FileInputFormat<GradoopId> {

  /**
   * Reusable array
   */
  private final byte[] bytes;

  /**
   * Default constructor
   */
  public DataSourceGradoopId() {
    bytes = new byte[GradoopId.ID_SIZE];
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return stream.available()>0;
  }

  @Override
  public GradoopId nextRecord(GradoopId gradoopId) throws IOException {
    stream.read(bytes);
    return GradoopId.fromByteArray(bytes);
  }

}
