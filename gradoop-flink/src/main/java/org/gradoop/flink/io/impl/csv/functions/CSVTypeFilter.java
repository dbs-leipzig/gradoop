package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.csv.pojos.Csv;

import java.util.List;


public class CSVTypeFilter implements FilterFunction<Tuple2<Csv, List<String>>> {
  private Class type;

  public CSVTypeFilter(Class type) {
    this.type = type;
  }

  @Override
  public boolean filter(Tuple2<Csv, List<String>> tuple) throws Exception {
    if (tuple.f0.getGraphhead() != null
      && type.isInstance(tuple.f0.getGraphhead())) {
      return true;
    } else if (tuple.f0.getVertex() != null
      && type.isInstance(tuple.f0.getVertex())) {
      return true;
    } else if (tuple.f0.getEdge() != null
      && type.isInstance(tuple.f0.getEdge())) {
      return true;
    }
    return false;
  }
}