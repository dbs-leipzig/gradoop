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

package org.gradoop.model.impl.operators.collection;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;

import java.util.Iterator;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public abstract class AbstractBinaryCollectionToCollectionOperator<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements BinaryCollectionToCollectionOperator<VD, ED, GD> {

  /**
   * Gelly graph representing the first input collection.
   */
  protected Graph<Long, VD, ED> firstGraph;
  /**
   * Gelly graph representing the second input collection.
   */
  protected Graph<Long, VD, ED> secondGraph;

  /**
   * Graph data of the first input collection.
   */
  protected DataSet<Subgraph<Long, GD>> firstSubgraphs;
  /**
   * Graph data of the second input collection.
   */
  protected DataSet<Subgraph<Long, GD>> secondSubgraphs;

  /**
   * Flink execution environment.
   */
  protected ExecutionEnvironment env;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    GraphCollection<VD, ED, GD> firstCollection,
    GraphCollection<VD, ED, GD> secondCollection) throws Exception {

    // do some init stuff for the actual operator
    env = firstCollection.getVertices().getExecutionEnvironment();
    firstGraph = Graph.fromDataSet(firstCollection.getVertices(),
      firstCollection.getEdges(),
      env);
    firstSubgraphs = firstCollection.getSubgraphs();

    secondGraph = Graph.fromDataSet(secondCollection.getVertices(),
      secondCollection.getEdges(),
      env);
    secondSubgraphs = secondCollection.getSubgraphs();

    final DataSet<Subgraph<Long, GD>> newSubgraphs = computeNewSubgraphs();
    final DataSet<Vertex<Long, VD>> newVertices =
      computeNewVertices(newSubgraphs);
    final DataSet<Edge<Long, ED>> newEdges = computeNewEdges(newVertices);

    return new GraphCollection<>(Graph.fromDataSet(newVertices, newEdges, env),
      newSubgraphs, firstCollection.getVertexDataFactory(),
      firstCollection.getEdgeDataFactory(),
      firstCollection.getGraphDataFactory(), env);
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newSubgraphs graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<Vertex<Long, VD>> computeNewVertices(
    DataSet<Subgraph<Long, GD>> newSubgraphs) throws Exception;

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<Subgraph<Long, GD>> computeNewSubgraphs();

  /**
   * Overriden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<Edge<Long, ED>> computeNewEdges(
    DataSet<Vertex<Long, VD>> newVertices);

  /**
   * Checks if the number of grouped elements equals a given expected size.
   *
   * @param <GD> graph data type
   * @see Intersect
   */
  protected static class SubgraphGroupReducer<GD extends GraphData> implements
    GroupReduceFunction<Subgraph<Long, GD>, Subgraph<Long, GD>> {

    /**
     * User defined expectedGroupSize.
     */
    private final long expectedGroupSize;

    /**
     * Creates new group reducer.
     *
     * @param expectedGroupSize expected group size
     */
    public SubgraphGroupReducer(long expectedGroupSize) {
      this.expectedGroupSize = expectedGroupSize;
    }

    /**
     * If the number of elements in the group is equal to the user expected
     * group size, the subgraph will be returned.
     *
     * @param iterable  graph data
     * @param collector output collector (contains 0 or 1 graph)
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<Subgraph<Long, GD>> iterable,
      Collector<Subgraph<Long, GD>> collector) throws Exception {
      Iterator<Subgraph<Long, GD>> iterator = iterable.iterator();
      long count = 0L;
      Subgraph<Long, GD> s = null;
      while (iterator.hasNext()) {
        s = iterator.next();
        count++;
      }
      if (count == expectedGroupSize) {
        collector.collect(s);
      }
    }
  }

  /**
   * Returns only the edge as result of an edge vertex join.
   *
   * @param <VD> vertex data type
   * @param <ED> edge data type
   */
  protected static class EdgeJoinFunction<VD extends VertexData, ED extends
    EdgeData> implements
    JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>> {

    /**
     * Returns only the left tuple of the join result.
     *
     * @param leftTuple  edge
     * @param rightTuple vertex
     * @return edge
     * @throws Exception
     */
    @Override
    public Edge<Long, ED> join(Edge<Long, ED> leftTuple,
      Vertex<Long, VD> rightTuple) throws Exception {
      return leftTuple;
    }
  }

  /**
   * Creates a {@link Tuple2} from the given input and a given Long value.
   *
   * @param <C> input type
   */
  protected static class Tuple2LongMapper<C> implements
    MapFunction<C, Tuple2<C, Long>> {

    /**
     * Value to add to the resulting tuple.
     */
    private final Long secondField;

    /**
     * Creates this mapper
     *
     * @param secondField user defined long value
     */
    public Tuple2LongMapper(Long secondField) {
      this.secondField = secondField;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<C, Long> map(C c) throws Exception {
      return new Tuple2<>(c, secondField);
    }
  }

  /**
   * Returns the identifier of the subgraph in the given tuple.
   *
   * @param <GD> graph data type
   * @param <C>  type of second element in tuple
   */
  protected static class SubgraphTupleKeySelector<GD extends GraphData, C>
    implements
    KeySelector<Tuple2<Subgraph<Long, GD>, C>, Long> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Long getKey(Tuple2<Subgraph<Long, GD>, C> subgraph) throws
      Exception {
      return subgraph.f0.getId();
    }
  }
}
