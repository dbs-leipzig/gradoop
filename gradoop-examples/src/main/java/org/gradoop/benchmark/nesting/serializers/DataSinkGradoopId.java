package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DataSinkGradoopId extends FileOutputFormat<GradoopId> {

  public DataSinkGradoopId(Path outputPath) {
    super(outputPath);
  }

  @Override
  public void writeRecord(GradoopId gradoopId) throws IOException {
    stream.write(gradoopId.toByteArray());
  }
}
