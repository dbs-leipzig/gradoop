/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Domain;

/**
 * Collects all csv objects from a datasource.
 */
public class DatasourceToCsv implements FlatMapFunction<Datasource, CsvExtension> {

  @Override
  public void flatMap(Datasource datasource, Collector<CsvExtension> collector) throws
    Exception {
    for (Domain domain : datasource.getDomain()) {
      for (CsvExtension csv : domain.getCsv()) {
        csv.setDatasourceName(datasource.getName());
        csv.setDomainName(domain.getName());
        collector.collect(csv);
      }
    }
  }
}
