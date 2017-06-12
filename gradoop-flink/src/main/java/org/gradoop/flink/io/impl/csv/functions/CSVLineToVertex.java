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

import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * /**
 * Creates a {@link Vertex} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * vertex-id;vertex-label;value_1|value_2|...|value_n
 */
public class CSVLineToVertex extends CSVLineToElement<Vertex> {
  /**
   * Used to instantiate the vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Constructor
   *
   * @param epgmVertexFactory EPGM vertex factory
   */
  public CSVLineToVertex(EPGMVertexFactory<Vertex> epgmVertexFactory) {
    this.vertexFactory = epgmVertexFactory;
  }

  @Override
  public Vertex map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 3);
    return vertexFactory.initVertex(
      GradoopId.fromString(tokens[0]),
      tokens[1],
      parseProperties(tokens[1], tokens[2]));
  }
}
