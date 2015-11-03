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

package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;

import java.util.Iterator;

/**
 * Abstract operator implementation which can be used with binary graph to
 * graph operators.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public abstract class AbstractBinaryGraphToGraphOperator<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements BinaryGraphToGraphOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> firstGraph,
    LogicalGraph<VD, ED, GD> secondGraph) {
    return executeInternal(firstGraph, secondGraph);
  }

  /**
   * Executes the actual operator implementation.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return operator result
   */
  protected abstract LogicalGraph<VD, ED, GD> executeInternal(
    LogicalGraph<VD, ED, GD> firstGraph, LogicalGraph<VD, ED, GD> secondGraph);

  /**
   * Used for {@link Overlap} and {@link Exclusion}.
   * <p>
   * Checks if the number of grouped, duplicate vertices is equal to a
   * given amount. If yes, reducer returns the vertex.
   * <p>
   * Furthermore, to realize exclusion, if two graphs are given, the method
   * checks if the vertex is contained in the first (include graph) but not
   * in the other graph (preclude graph). If this is the case, the vertex
   * gets returned.
   */
  protected static class VertexGroupReducer<VD extends VertexData> implements
    GroupReduceFunction<VD, VD> {

    /**
     * Number of times, a vertex must occur inside a group
     */
    private long amount;

    /**
     * Graph, a vertex must be part of
     */
    private Long includedGraphID;

    /**
     * Graph, a vertex must not be part of
     */
    private Long precludedGraphID;

    /**
     * Creates group reducer.
     *
     * @param amount number of times, a vertex must occur inside a group
     */
    public VertexGroupReducer(long amount) {
      this(amount, null, null);
    }

    /**
     * Creates group reducer
     *
     * @param amount           number of number of times, a vertex must occur
     *                         inside a group
     * @param includedGraphID  graph, a vertex must be part of
     * @param precludedGraphID graph, a vertex must not be part of
     */
    public VertexGroupReducer(long amount, Long includedGraphID,
      Long precludedGraphID) {
      this.amount = amount;
      this.includedGraphID = includedGraphID;
      this.precludedGraphID = precludedGraphID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VD> iterable,
      Collector<VD> collector) throws Exception {
      Iterator<VD> iterator = iterable.iterator();
      long count = 0L;
      VD vertex = null;
      while (iterator.hasNext()) {
        vertex = iterator.next();
        count++;
      }
      if (count == amount) {
        if (includedGraphID != null && precludedGraphID != null) {
          assert vertex != null;
          if (vertex.getGraphs().contains(includedGraphID) &&
            !vertex.getGraphs().contains(precludedGraphID)) {
            collector.collect(vertex);
          }
        } else {
          collector.collect(vertex);
        }
      }
    }
  }

  /**
   * Used for {@link Overlap} and {@link Exclusion}.
   * <p>
   * Used to check if the number of grouped, duplicate edges is equal to a
   * given amount. If yes, reducer returns the edge.
   */
  protected static class EdgeGroupReducer<ED extends EdgeData> implements
    GroupReduceFunction<ED, ED> {

    /**
     * Number of group elements that must be reached.
     */
    private final long amount;

    /**
     * Creates group reducer.
     *
     * @param amount number of group elements that must be reached to collect
     *               the vertex
     */
    public EdgeGroupReducer(long amount) {
      this.amount = amount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<ED> iterable,
      Collector<ED> collector) throws Exception {
      Iterator<ED> iterator = iterable.iterator();
      long count = 0L;
      ED edge = null;
      while (iterator.hasNext()) {
        edge = iterator.next();
        count++;
      }
      if (count == amount) {
        collector.collect(edge);
      }
    }
  }
}
