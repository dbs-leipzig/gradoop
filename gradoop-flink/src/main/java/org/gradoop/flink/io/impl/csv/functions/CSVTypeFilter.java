package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.io.impl.csv.pojos.Csv;

import java.util.List;


public class CSVTypeFilter implements FilterFunction<EPGMElement> {
  private Class type;

  public CSVTypeFilter(Class type) {
    this.type = type;
  }

  @Override
  public boolean filter(EPGMElement element) throws Exception {
    if (type.isInstance(element)) {
      return true;
    } else if (type.isInstance(element)) {
      return true;
    } else if (type.isInstance(element)) {
      return true;
    }
    return false;
  }
}