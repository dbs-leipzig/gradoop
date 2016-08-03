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

package org.gradoop.flink.io.impl.edgelist.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

/**
 * (vertexId, label) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CreateImportVertex
  implements MapFunction<Tuple2<Long, String>, ImportVertex<Long>> {

  /**
   * reused ImportVertex
   */
  private ImportVertex<Long> reuseVertex;

  /**
   * PropertyKey of property value
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey used PropertyKey
   */
  public CreateImportVertex(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new ImportVertex<>();
    reuseVertex.setLabel(GConstants.DEFAULT_VERTEX_LABEL);
  }

  @Override
  public ImportVertex<Long> map(Tuple2<Long, String> inputTuple) throws
    Exception {
    reuseVertex.setId(inputTuple.f0);
    PropertyList properties = PropertyList.createWithCapacity(1);
    properties.set(propertyKey, inputTuple.f1);
    reuseVertex.setProperties(properties);
    return reuseVertex;
  }
}
