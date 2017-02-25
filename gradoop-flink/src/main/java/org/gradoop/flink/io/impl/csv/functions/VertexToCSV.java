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

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Converts an {@link Vertex} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class VertexToCSV extends ElementToCSV<Vertex, Tuple3<String, String, String>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple3<String, String, String> tuple = new Tuple3<>();

  @Override
  public Tuple3<String, String, String> map(Vertex vertex) throws Exception {
    tuple.f0 = vertex.getId().toString();
    tuple.f1 = vertex.getLabel();
    tuple.f2 = getPropertyString(vertex);
    return tuple;
  }
}
