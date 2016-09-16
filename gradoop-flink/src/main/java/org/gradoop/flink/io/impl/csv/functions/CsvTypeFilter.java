package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;


public class CsvTypeFilter implements FilterFunction<Datasource> {
  @Override
  public boolean filter(Datasource csv) throws Exception {
    return false;
  }
}
