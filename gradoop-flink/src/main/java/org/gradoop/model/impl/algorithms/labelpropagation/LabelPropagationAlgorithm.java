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

package org.gradoop.model.impl.algorithms.labelpropagation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .LPMessageFunction;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .LPUpdateFunction;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Implementation of the Label Propagation Algorithm:
 * The input graph as adjacency list contains the information about the
 * vertex (id), value (label) and its edges to neighbors.
 * <p/>
 * In super step 0 each vertex will propagate its value within his neighbors
 * <p/>
 * In the remaining super steps each vertex will adopt the value of the
 * majority of their neighbors or the smallest one if there are just one
 * neighbor. If a vertex adopt a new value it'll propagate the new one again.
 * <p/>
 * The computation will terminate if no new values are assigned.
 */
public class LabelPropagationAlgorithm
  implements GraphAlgorithm<GradoopId, LPVertexValue, NullValue,
    Graph<GradoopId, LPVertexValue, NullValue>> {
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int counter to define maximal Iterations
   */
  public LabelPropagationAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * Graph run method to start the VertexCentricIteration
   *
   * @param graph graph that should be used for EPGMLabelPropagation
   * @return gelly Graph with labeled vertices
   * @throws Exception
   */
  @Override
  public Graph<GradoopId, LPVertexValue, NullValue> run(
    Graph<GradoopId, LPVertexValue, NullValue> graph) throws Exception {
    // initialize vertex values and run the Vertex Centric Iteration
    Graph<GradoopId, LPVertexValue, NullValue> gellyGraph =
      graph.getUndirected();
    return gellyGraph
      .runVertexCentricIteration(
        new LPUpdateFunction(),
        new LPMessageFunction(),
        maxIterations);
  }
}
