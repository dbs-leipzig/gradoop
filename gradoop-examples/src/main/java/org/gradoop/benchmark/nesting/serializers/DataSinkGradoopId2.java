package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Created by vasistas on 08/04/17.
 */
public class DataSinkGradoopId2 implements TextFormatter<GradoopId> {
  @Override
  public String format(GradoopId gradoopId) {
    return new String(gradoopId.toByteArray());
  }
}
