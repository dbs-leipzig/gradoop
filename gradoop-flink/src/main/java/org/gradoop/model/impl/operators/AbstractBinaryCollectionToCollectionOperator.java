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
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;
import org.gradoop.model.operators.BinaryCollectionToCollectionOperator;

import java.util.Iterator;

public abstract class AbstractBinaryCollectionToCollectionOperator<VD extends
  VertexData, ED extends EdgeData, GD extends GraphData> implements
  BinaryCollectionToCollectionOperator<VD, ED, GD> {

  protected Graph<Long, VD, ED> firstGraph;
  protected Graph<Long, VD, ED> secondGraph;

  protected DataSet<Subgraph<Long, GD>> firstSubgraphs;
  protected DataSet<Subgraph<Long, GD>> secondSubgraphs;

  protected ExecutionEnvironment env;

  @Override
  public EPGraphCollection<VD, ED, GD> execute(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondCollection) throws Exception {

    firstGraph = firstCollection.getGellyGraph();
    firstSubgraphs = firstCollection.getSubgraphs();
    secondGraph = secondCollection.getGellyGraph();
    secondSubgraphs = secondCollection.getSubgraphs();
    env = firstGraph.getContext();

    return executeInternal(firstCollection, secondCollection);
  }

  protected abstract EPGraphCollection<VD, ED, GD> executeInternal(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondGraphCollection) throws Exception;

  protected static class SubgraphGroupReducer<GD extends GraphData> implements
    GroupReduceFunction<Subgraph<Long, GD>, Subgraph<Long, GD>> {

    /**
     * number of times a vertex must occur inside a group
     */
    private long amount;

    public SubgraphGroupReducer(long amount) {
      this.amount = amount;
    }

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
      if (count == amount) {
        collector.collect(s);
      }
    }
  }

  protected static class EdgeJoinFunction<VD extends VertexData, ED extends
    EdgeData> implements
    JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>> {

    @Override
    public Edge<Long, ED> join(Edge<Long, ED> leftTuple,
      Vertex<Long, VD> rightTuple) throws Exception {
      return leftTuple;
    }
  }

  protected static class Tuple2LongMapper<C> implements
    MapFunction<C, Tuple2<C, Long>> {

    private Long secondField;

    public Tuple2LongMapper(Long secondField) {
      this.secondField = secondField;
    }

    @Override
    public Tuple2<C, Long> map(C c) throws Exception {
      return new Tuple2<>(c, secondField);
    }
  }

  protected static class SubgraphTupleKeySelector<GD extends GraphData, C>
    implements
    KeySelector<Tuple2<Subgraph<Long, GD>, C>, Long> {
    @Override
    public Long getKey(Tuple2<Subgraph<Long, GD>, C> subgraph) throws
      Exception {
      return subgraph.f0.getId();
    }
  }

}
