package org.gradoop.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class VertexLabelFilter implements FilterFunction<Vertex> {

  private String[] labels;
  private boolean pass;

  public VertexLabelFilter(String... label) {
    this.labels = label;
  }

  @Override
  public boolean filter(Vertex vertex) throws Exception {
    for (String label: labels) {
      pass = vertex.getLabel().equals(label);
      if (pass) break;
    }
    return pass;
  }
}
