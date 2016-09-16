package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.io.impl.csv.pojos.Csv;


public class CsvTypeFilter implements FilterFunction<Csv> {
  private Class type;

  public CsvTypeFilter(Class type) {
    this.type = type;
  }

  @Override
  public boolean filter(Csv csv) throws Exception {
    if (csv.getGraphhead() != null && type.isInstance(csv.getGraphhead())) {
      return true;
    } else if (csv.getVertex() != null && type.isInstance(csv.getVertex())) {
      return true;
    } else if (csv.getEdge() != null && type.isInstance(csv.getEdge())) {
      return true;
    }
    return false;
  }
}
