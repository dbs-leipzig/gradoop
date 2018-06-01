package org.gradoop.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;

public class EdgeLabelFilter implements FilterFunction<Edge> {

  private String[] labels;
  private boolean pass;

  public EdgeLabelFilter(String... label) {
    this.labels = label;
  }

  @Override
  public boolean filter(Edge edge) throws Exception {
    for (String label: labels) {
      pass = edge.getLabel().equals(label);
      if (pass) break;
    }
    return pass;
  }
}
