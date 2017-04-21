package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;

import java.io.IOException;
import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 */
public class SerializeVertexInformation<VH extends EPGMElement> implements MapFunction<VH, String> {

  /**
   * Reusable builder
   */
  private StringBuilder sb;

  /**
   * Default constructor
   */
  public SerializeVertexInformation() {
    sb = new StringBuilder();
  }

  @Override
  public String map(VH vertex) throws Exception {
    sb.setLength(0);
    sb.append(vertex.getId().toString())
      .append(',')
      .append(vertex.getLabel() == null ? "" : ("\"" + vertex.getLabel() + "\""));
    if (vertex.getProperties() != null) {
      Iterator<Property> properties = vertex.getProperties().iterator();
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
