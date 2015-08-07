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

package org.gradoop.model.impl.operators;

import org.apache.commons.lang.NotImplementedException;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

/**
 * Creates a projected version of the logical graph using the user defined
 * vertex and edge data projection functions.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class Projection<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  UnaryGraphToGraphOperator<VD, ED, GD> {

  /**
   * Vertex projection function.
   */
  private final UnaryFunction<VD, VD> vertexFunc;
  /**
   * Edge projection function.
   */
  private final UnaryFunction<ED, ED> edgeFunc;

  /**
   * Creates new projection.
   *
   * @param vertexFunc vertex projection function
   * @param edgeFunc   edge projection function
   */
  public Projection(UnaryFunction<VD, VD> vertexFunc,
    UnaryFunction<ED, ED> edgeFunc) {
    this.vertexFunc = vertexFunc;
    this.edgeFunc = edgeFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> graph) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "Projection";
  }
}
