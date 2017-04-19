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

package org.gradoop.flink.io.impl.xmlbasedcsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.xmlbasedcsv.pojos.CsvExtension;

/**
 * Maps the csv meta object to each line from the csv file.
 */
public class CSVToContent implements MapFunction<String, Tuple2<CsvExtension, String>> {
  /**
   * Csv meta object.
   */
  private CsvExtension csv;

  /**
   * Valued Constructor.
   *
   * @param csv object, containing meta information
   */
  public CSVToContent(CsvExtension csv) {
    this.csv = csv;
  }

  @Override
  public Tuple2<CsvExtension, String> map(String s) throws Exception {
    return new Tuple2<CsvExtension, String>(csv, s);
  }
}
