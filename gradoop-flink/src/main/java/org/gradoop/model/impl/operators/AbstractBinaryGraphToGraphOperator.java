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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;

import java.util.Iterator;

/**
 * Abstract operator implementation which can be used with binary graph to
 * graph operators.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public abstract class AbstractBinaryGraphToGraphOperator<VD extends
  VertexData, ED extends EdgeData, GD extends GraphData> implements
  BinaryGraphToGraphOperator<VD, ED, GD> {

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
    GroupReduceFunction<Vertex<Long, VD>, Vertex<Long, VD>> {

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
    public void reduce(Iterable<Vertex<Long, VD>> iterable,
      Collector<Vertex<Long, VD>> collector) throws Exception {
      Iterator<Vertex<Long, VD>> iterator = iterable.iterator();
      long count = 0L;
      Vertex<Long, VD> v = null;
      while (iterator.hasNext()) {
        v = iterator.next();
        count++;
      }
      if (count == amount) {
        if (includedGraphID != null && precludedGraphID != null) {
          assert v != null;
          if (v.getValue().getGraphs().contains(includedGraphID) &&
            !v.getValue().getGraphs().contains(precludedGraphID)) {
            collector.collect(v);
          }
        } else {
          collector.collect(v);
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
    GroupReduceFunction<Edge<Long, ED>, Edge<Long, ED>> {

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
    public void reduce(Iterable<Edge<Long, ED>> iterable,
      Collector<Edge<Long, ED>> collector) throws Exception {
      Iterator<Edge<Long, ED>> iterator = iterable.iterator();
      long count = 0L;
      Edge<Long, ED> e = null;
      while (iterator.hasNext()) {
        e = iterator.next();
        count++;
      }
      if (count == amount) {
        collector.collect(e);
      }
    }
  }

  /**
   * Adds a given graph ID to the vertex and returns it.
   *
   * @param <VD> vertex data type
   */
  protected static class VertexToGraphUpdater<VD extends VertexData> implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {

    /**
     * Graph identifier to add.
     */
    private final long newGraphID;

    /**
     * Creates map function
     *
     * @param newGraphID graph identifier to add to the vertex
     */
    public VertexToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> v) throws Exception {
      v.getValue().addGraph(newGraphID);
      return v;
    }
  }

  /**
   * Adds a given graph ID to the edge and returns it.
   *
   * @param <ED> edge data type
   */
  protected static class EdgeToGraphUpdater<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Edge<Long, ED>> {

    /**
     * Graph identifier to add.
     */
    private final long newGraphID;

    /**
     * Creates map function
     *
     * @param newGraphID graph identifier to add
     */
    public EdgeToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge<Long, ED> map(Edge<Long, ED> e) throws Exception {
      e.getValue().addGraph(newGraphID);
      return e;
    }
  }
}
