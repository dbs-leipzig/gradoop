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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * Creates an {@link Edge} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * edge-id;source-id;target-id;edge-label;value_1|value_2|...|value_n
 */
public class CSVEdgeToEdge extends CSVLineToElement<Edge> {
  /**
   * Used to instantiate the edge.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Constructor.
   *
   * @param edgeFactory EPGM edge factory
   */
  public CSVEdgeToEdge(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public Edge map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 5);
    return edgeFactory.initEdge(GradoopId.fromString(tokens[0]),
      tokens[3],
      GradoopId.fromString(tokens[1]),
      GradoopId.fromString(tokens[2]),
      parseProperties(tokens[3], tokens[4]));
  }
}
