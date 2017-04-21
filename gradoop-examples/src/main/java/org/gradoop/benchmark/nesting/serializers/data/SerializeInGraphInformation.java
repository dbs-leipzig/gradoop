package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;

import java.io.IOException;
import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 */
public class SerializeInGraphInformation<GE extends GraphElement>
  implements MapFunction<GE, String> {


  /**
   * Reusable builder
   */
  private StringBuilder sb;

  /**
   * Default constructor
   */
  public SerializeInGraphInformation() {
    sb = new StringBuilder();
  }

  @Override
  public String map(GE graphElement) throws Exception {
    sb.setLength(0);
    sb.append(graphElement.getId().toString());
    if (graphElement.getGraphIds() != null) {
      Iterator<GradoopId> graphIds = graphElement.getGraphIds().iterator();
      while (graphIds.hasNext()) {
        GradoopId p = graphIds.next();
        sb.append(',').append(p.toString());
      }
    }
    return sb.toString();
  }
}
