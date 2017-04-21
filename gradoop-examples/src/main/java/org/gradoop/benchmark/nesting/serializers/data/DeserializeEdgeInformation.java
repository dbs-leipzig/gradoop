package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Serializing a vertex in a proper way
 */
public class DeserializeEdgeInformation implements MapFunction<String, Edge> {

  /**
   * Reusable element
   */
  private final Edge v;

  /**
   * Default constructor
   */
  public DeserializeEdgeInformation() {
    v = new Edge();
    v.setProperties(new Properties());
  }

  @Override
  public Edge map(String value) throws Exception {
    String[] array = value.split(",");
    v.setId(GradoopId.fromString(array[0]));
    v.setSourceId(GradoopId.fromString(array[1]));
    v.setTargetId(GradoopId.fromString(array[2]));
    if (array[3].startsWith("\"") && array[3].endsWith("\"")) {
      v.setLabel(array[3].substring(1, array[3].length() - 1));
    } else {
      v.setLabel(null);
    }
    v.getProperties().clear();
    if (array.length > 4) {
      for (int i=4; i<array.length; i += 2) {
        v.setProperty(array[i], array[i+1]);
      }
    }
    return v;
  }
}
