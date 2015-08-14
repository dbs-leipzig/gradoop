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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.operators;

import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;

/**
 * Creates a {@link GraphCollection} based on one {@link LogicalGraph}.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public interface UnaryGraphToCollectionOperator<VD extends VertexData, ED
  extends EdgeData, GD extends GraphData> extends
  Operator {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  GraphCollection<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> graph) throws
    Exception;
}
