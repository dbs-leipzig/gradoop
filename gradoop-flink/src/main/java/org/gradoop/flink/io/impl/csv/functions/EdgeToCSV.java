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
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Converts an {@link Edge} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f3")
public class EdgeToCSV extends ElementToCSV<Edge, Tuple5<String, String, String, String, String>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple5<String, String, String, String, String> tuple = new Tuple5<>();

  @Override
  public Tuple5<String, String, String, String, String> map(Edge edge) throws Exception {
    tuple.f0 = edge.getId().toString();
    tuple.f1 = edge.getSourceId().toString();
    tuple.f2 = edge.getTargetId().toString();
    tuple.f3 = edge.getLabel();
    tuple.f4 = getPropertyString(edge);
    return tuple;
  }
}
