package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Property;

import java.io.IOException;
import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 */
public class SerializeEdgeInformation implements MapFunction<Edge, String> {

  /**
   * Reusable builder
   */
  private StringBuilder sb;

  /**
   * Default constructor
   */
  public SerializeEdgeInformation() {
    sb = new StringBuilder();
  }

  @Override
  public String map(Edge edge) throws Exception {
    sb.setLength(0);
    sb
      .append(edge.getId().toString())
      .append(',')
      .append(edge.getSourceId().toString())
      .append(',')
      .append(edge.getTargetId().toString())
      .append(',')
      .append(edge.getLabel() == null ? "" : ("\"" + edge.getLabel() + "\""));
    if (edge.getProperties() != null) {
      Iterator<Property> properties = edge.getProperties().iterator();
      while (properties.hasNext()) {
        Property p = properties.next();
        sb.append(',')
          .append(p.getKey())
          .append(',')
          .append(p.getValue());
      }
    }
    return sb.toString();
  }
}
