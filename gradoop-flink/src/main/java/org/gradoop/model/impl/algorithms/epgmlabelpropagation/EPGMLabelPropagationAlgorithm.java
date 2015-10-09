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

package org.gradoop.model.impl.algorithms.epgmlabelpropagation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.algorithms.epgmlabelpropagation.functions
  .LPMessageFunction;
import org.gradoop.model.impl.algorithms.epgmlabelpropagation.functions
  .LPUpdateFunction;

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
 *
 * @param <VD> VertexData contains information about the vertex
 * @param <ED> EdgeData contains information about all edges of the vertex
 */
public class EPGMLabelPropagationAlgorithm<
  VD extends VertexData,
  ED extends EdgeData>
  implements GraphAlgorithm<Long, VD, ED, Graph<Long, VD, ED>> {
  /**
   * Vertex property key where the resulting label is stored.
   */
  public static final String CURRENT_VALUE = "value";
  /**
   * Vertex property key where the lasat label is stored
   */
  public static final String LAST_VALUE = "lastvalue";
  /**
   * Vertex property key where stabilization counter is stored
   */
  public static final String STABILIZATION_COUNTER = "stabilization.counter";
  /**
   * Vertex property key where the stabilization maxima is stored
   */
  public static final String STABILIZATION_MAX = "stabilization.max";
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int counter to define maximal Iterations
   */
  public EPGMLabelPropagationAlgorithm(int maxIterations) {
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
  public Graph<Long, VD, ED> run(Graph<Long, VD, ED> graph) throws Exception {
    // initialize vertex values and run the Vertex Centric Iteration
    Graph<Long, VD, ED> epGraph = graph.getUndirected();
    return epGraph
      .runVertexCentricIteration(
        new LPUpdateFunction<VD>(),
        new LPMessageFunction<VD, ED>(),
        maxIterations);
  }
}
