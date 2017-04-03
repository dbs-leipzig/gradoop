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
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * (vertexId) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 *
 * @param <K> comparable key
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CreateImportVertex<K extends Comparable<K>>
  implements MapFunction<Tuple1<K>, ImportVertex<K>> {
  /**
   * Reduce object instantiations
   */
  private final ImportVertex<K> reuseVertex;

  /**
   * Constructor
   */
  public CreateImportVertex() {
    this.reuseVertex = new ImportVertex<>();
    this.reuseVertex.setLabel(GConstants.DEFAULT_VERTEX_LABEL);
  }

  @Override
  public ImportVertex<K> map(Tuple1<K> value) throws Exception {
    reuseVertex.f0 = value.f0;
    return reuseVertex;
  }
}
