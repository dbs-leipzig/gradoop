package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;

/**
 * Writes <GradoopId,GradoopId> pairs to bytes
 */
public class DataSinkTupleOfGradoopId extends FileOutputFormat<Tuple2<GradoopId,GradoopId>> {

  public DataSinkTupleOfGradoopId(Path outputPath) {
    super(outputPath);
  }

  @Override
  public void writeRecord(Tuple2<GradoopId,GradoopId> gradoopId) throws IOException {
    stream.write(gradoopId.f0.toByteArray());
    stream.write(gradoopId.f1.toByteArray());
  }
}
