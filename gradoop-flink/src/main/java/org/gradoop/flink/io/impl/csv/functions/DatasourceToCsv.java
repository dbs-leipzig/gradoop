package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Domain;

/**
 * Created by Stephan on 16.09.16.
 */
public class DatasourceToCsv implements FlatMapFunction<Datasource, Csv> {

  @Override
  public void flatMap(Datasource datasource, Collector<Csv> collector) throws
    Exception {
    for (Domain domain : datasource.getDomain()) {
      for (Csv csv : domain.getCsv()) {
        csv.setDatasourceName(datasource.getName());
        csv.setDomainName(domain.getName());
        collector.collect(csv);
      }
    }
  }
}
