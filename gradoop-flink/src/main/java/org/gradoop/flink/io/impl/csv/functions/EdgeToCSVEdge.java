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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;

/**
 * Converts an {@link Edge} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f3")
public class EdgeToCSVEdge extends ElementToCSV<Edge, CSVEdge> {
  /**
   * Reduce object instantiations
   */
  private final CSVEdge csvEdge = new CSVEdge();

  @Override
  public CSVEdge map(Edge edge) throws Exception {
    csvEdge.setId(edge.getId().toString());
    csvEdge.setSourceId(edge.getSourceId().toString());
    csvEdge.setTargetId(edge.getTargetId().toString());
    csvEdge.setLabel(edge.getLabel());
    csvEdge.setProperties(getPropertyString(edge));
    return csvEdge;
  }
}
