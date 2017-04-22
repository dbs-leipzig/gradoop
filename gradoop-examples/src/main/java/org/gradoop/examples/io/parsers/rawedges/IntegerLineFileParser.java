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

package org.gradoop.examples.io.parsers.rawedges;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Parses a file containing the edge information
 */
public class IntegerLineFileParser extends FileReaderForParser {

  /**
   * Default constructor
   */
  public IntegerLineFileParser() {
    super("\n");
  }

  /**
   * Initializes the graph as a GraphClob
   * @param isUndirected  Checks if the graph is undirected
   * @param conf          Configuration
   * @return              The instantiated graph
   */
  public DataSet<List<String>> getDataset(boolean isUndirected, GradoopFlinkConfig conf) {
    return readAsStringDataSource().map(new NumberTokenizer());
  }

}
