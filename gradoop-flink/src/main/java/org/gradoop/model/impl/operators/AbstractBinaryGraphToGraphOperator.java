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
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;

import java.util.Iterator;

public abstract class AbstractBinaryGraphToGraphOperator<VD extends
  VertexData, ED extends EdgeData, GD extends GraphData> implements
  BinaryGraphToGraphOperator<VD, ED, GD> {

  @Override
  public EPGraph<VD, ED, GD> execute(EPGraph<VD, ED, GD> firstGraph,
    EPGraph<VD, ED, GD> secondGraph) {
    return executeInternal(firstGraph, secondGraph);
  }

  protected abstract EPGraph<VD, ED, GD> executeInternal(
    EPGraph<VD, ED, GD> firstGraph, EPGraph<VD, ED, GD> secondGraph);

  /**
   * Used for {@code EPGraph.overlap()} and {@code EPGraph.exclude()}
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
     * number of times a vertex must occur inside a group
     */
    private long amount;

    /**
     * graph, a vertex must be part of
     */
    private Long includedGraphID;

    /**
     * graph, a vertex must not be part of
     */
    private Long precludedGraphID;

    public VertexGroupReducer(long amount) {
      this(amount, null, null);
    }

    public VertexGroupReducer(long amount, Long includedGraphID,
      Long precludedGraphID) {
      this.amount = amount;
      this.includedGraphID = includedGraphID;
      this.precludedGraphID = precludedGraphID;
    }

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
   * Used for {@code EPGraph.overlap()} and {@code EPGraph.exclude()}
   * <p>
   * Used to check if the number of grouped, duplicate edges is equal to a
   * given amount. If yes, reducer returns the vertex.
   */
  protected static class EdgeGroupReducer<ED extends EdgeData> implements
    GroupReduceFunction<Edge<Long, ED>, Edge<Long, ED>> {

    private long amount;

    public EdgeGroupReducer(long amount) {
      this.amount = amount;
    }

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
   */
  protected static class VertexToGraphUpdater<VD extends VertexData> implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {

    private final long newGraphID;

    public VertexToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> v) throws Exception {
      v.getValue().addGraph(newGraphID);
      return v;
    }
  }

  /**
   * Adds a given graph ID to the edge and returns it.
   */
  protected static class EdgeToGraphUpdater<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Edge<Long, ED>> {

    private final long newGraphID;

    public EdgeToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    @Override
    public Edge<Long, ED> map(Edge<Long, ED> e) throws Exception {
      e.getValue().addGraph(newGraphID);
      return e;
    }
  }
}
