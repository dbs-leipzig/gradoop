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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
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
public class CSVToVertex extends CSVToElement<Tuple3<String, String, String>, Vertex> {
  /**
   * Used to instantiate the vertex.
   */
  private final VertexFactory vertexFactory;

  /**
   * Constructor
   *
   * @param vertexFactory EPGM vertex factory
   */
  public CSVToVertex(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public Vertex map(Tuple3<String, String, String> csvVertex) throws Exception {
    return vertexFactory.initVertex(
      GradoopId.fromString(csvVertex.f0),
      csvVertex.f1,
      parseProperties(csvVertex.f1, csvVertex.f2.split(VALUE_DELIMITER)));
  }
}
