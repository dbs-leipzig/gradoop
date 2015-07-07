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

package org.gradoop.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.operators.EPGraphCollectionOperators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class EPGraphCollection implements
  EPGraphCollectionOperators<EPGraphData> {

  private ExecutionEnvironment env;

  private Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph;

  private DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs;

  public EPGraphCollection(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs,
    ExecutionEnvironment env) {
    this.graph = graph;
    this.subgraphs = subgraphs;
    this.env = env;
  }

  @Override
  public EPGraph getGraph(final Long graphID) throws Exception {
    // filter vertices and edges based on given graph id
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> subGraph = this.graph
      .filterOnVertices(new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {
        @Override
        public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          return vertex.getValue().getGraphs().contains(graphID);
        }
      });
    // get graph data based on graph id
    EPFlinkGraphData graphData =
      subgraphs.filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {
        @Override
        public boolean filter(Subgraph<Long, EPFlinkGraphData> graph) throws
          Exception {
          return graph.getId().equals(graphID);
        }
      }).collect().get(0).getValue();

    return EPGraph.fromGraph(subGraph, graphData, env);
  }

  @Override
  public EPGraphCollection getGraphs(final Long... identifiers) throws
    Exception {
    return getGraphs(Arrays.asList(identifiers));
  }

  public EPGraphCollection getGraphs(final List<Long> identifiers) throws
    Exception {
    ArrayList<Tuple1<Long>> idTuples = new ArrayList<>();
    for (Long id : identifiers) {
      idTuples.add(new Tuple1<>(id));
    }
    DataSet<Tuple1<Long>> ids = env.fromCollection(idTuples);
    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubGraphs =
      this.subgraphs.join(ids).where(0).equalTo(0).with(
        new JoinFunction<Subgraph<Long, EPFlinkGraphData>, Tuple1<Long>,
          Subgraph<Long, EPFlinkGraphData>>() {

          @Override
          public Subgraph<Long, EPFlinkGraphData> join(
            Subgraph<Long, EPFlinkGraphData> subgraph,
            Tuple1<Long> longTuple1) throws Exception {
            return subgraph;
          }
        });

    DataSet<Vertex<Long, EPFlinkVertexData>> vertices = this.graph.getVertices()
      .filter(new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {
        @Override
        public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          System.out.println(vertex.getValue().getGraphs());
          for (Long graph : vertex.getValue().getGraphs()) {
            if (identifiers.contains(graph)) {
              return true;
            }
          }
          return false;
        }
      });

    JoinFunction<Edge<Long, EPFlinkEdgeData>, Vertex<Long,
      EPFlinkVertexData>, Edge<Long, EPFlinkEdgeData>>
      joinFunc =
      new JoinFunction<Edge<Long, EPFlinkEdgeData>, Vertex<Long,
        EPFlinkVertexData>, Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public Edge<Long, EPFlinkEdgeData> join(
          Edge<Long, EPFlinkEdgeData> leftTuple,
          Vertex<Long, EPFlinkVertexData> rightTuple) throws Exception {
          return leftTuple;
        }
      };

    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      this.graph.getEdges().join(vertices).where(new EdgeSourceSelector())
        .equalTo(new VertexKeySelector()).with(joinFunc).join(vertices)
        .where(new EdgeTargetSelector()).equalTo(new VertexKeySelector())
        .with(joinFunc);

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);
    return new EPGraphCollection(newGraph, newSubGraphs, env);
  }

  @Override
  public Long getGraphCount() throws Exception {
    return this.subgraphs.count();
  }

  @Override
  public EPGraphCollection filter(
    final Predicate<EPGraphData> predicateFunction) throws Exception {
    // find subgraphs matching the predicate
    DataSet<Subgraph<Long, EPFlinkGraphData>> filteredSubgraphs = this.subgraphs
      .filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {

        @Override
        public boolean filter(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return predicateFunction
            .filter(longEPFlinkGraphDataSubgraph.getValue());
        }
      });

    // get the identifiers of these subgraphs
    final Collection<Long> graphIDs = filteredSubgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, Long>() {

        @Override
        public Long map(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return longEPFlinkGraphDataSubgraph.getId();
        }
      }).collect();

    // use graph ids to filter vertices from the actual graph structure
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> filteredGraph =
      this.graph.filterOnVertices(

        new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {
          @Override
          public boolean filter(
            Vertex<Long, EPFlinkVertexData> longEPFlinkVertexDataVertex) throws
            Exception {
            for (Long graphID : longEPFlinkVertexDataVertex.getValue()
              .getGraphs()) {
              if (graphIDs.contains(graphID)) {
                return true;
              }
            }
            return false;
          }
        });

    // create new graph
    return new EPGraphCollection(filteredGraph, filteredSubgraphs, env);
  }

  @Override
  public EPGraphCollection select(Predicate<EPGraph> predicateFunction) throws
    Exception {
    return null;
  }

  @Override
  public EPGraphCollection union(EPGraphCollection otherCollection) {
    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      this.subgraphs.union(otherCollection.subgraphs).distinct(0);
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices().union(otherCollection.graph.getVertices())
        .distinct(new VertexKeySelector());
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      this.graph.getEdges().union(otherCollection.graph.getEdges())
        .distinct(new EdgeKeySelector());

    Graph newGraph = Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  @Override
  public EPGraphCollection intersect(EPGraphCollection otherCollection) throws
    Exception {
    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      this.subgraphs.union(otherCollection.subgraphs)
        .groupBy(new SubgraphKeySelector())
        .reduceGroup(new SubgraphGroupReducer(2));

    DataSet<Vertex<Long, EPFlinkVertexData>> unionVertices =
      this.graph.getVertices();

    DataSet<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>> verticesWithGraphs =
      unionVertices.flatMap(
        new FlatMapFunction<Vertex<Long, EPFlinkVertexData>,
          Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>() {


          @Override
          public void flatMap(Vertex<Long, EPFlinkVertexData> vertexData,
            Collector<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>
              collector) throws
            Exception {
            for (Long graph : vertexData.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(vertexData, graph));
            }
          }
        });
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      verticesWithGraphs.join(newSubgraphs).where(1).equalTo(0).with(
        new JoinFunction<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>,
          Subgraph<Long, EPFlinkGraphData>, Vertex<Long, EPFlinkVertexData>>() {


          @Override
          public Vertex<Long, EPFlinkVertexData> join(
            Tuple2<Vertex<Long, EPFlinkVertexData>, Long> vertexLongTuple2,
            Subgraph<Long, EPFlinkGraphData> subgraph) throws Exception {
            return vertexLongTuple2.f0;
          }
        });

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();


    edges = edges.join(vertices).where(new EdgeSourceSelector()).equalTo(0)
      .with(new EdgeJoinFunction()).join(vertices)
      .where(new EdgeTargetSelector()).equalTo(0).with(new EdgeJoinFunction());

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  public EPGraphCollection alternateIntersect(
    EPGraphCollection otherCollection) throws Exception {


    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      this.subgraphs.union(otherCollection.subgraphs)
        .groupBy(new SubgraphKeySelector())
        .reduceGroup(new SubgraphGroupReducer(2));

    final List<Long> identifiers;
    identifiers = otherCollection.subgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, Long>() {
        @Override
        public Long map(Subgraph<Long, EPFlinkGraphData> subgraph) throws
          Exception {
          return subgraph.getId();
        }
      }).collect();


    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices();
    vertices =
      vertices.filter(new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {

        @Override
        public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          for (Long id : identifiers) {
            if (vertex.getValue().getGraphs().contains(id)) {
              return true;
            }
          }
          return false;
        }
      });


    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();


    edges = edges.join(vertices).where(new EdgeSourceSelector()).equalTo(0)
      .with(new EdgeJoinFunction()).join(vertices)
      .where(new EdgeTargetSelector()).equalTo(0).with(new EdgeJoinFunction());

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  @Override
  public EPGraphCollection difference(EPGraphCollection otherCollection) {

    DataSet<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> thisGraphs =
      this.subgraphs
        .map(new Tuple2LongMapper<Subgraph<Long, EPFlinkGraphData>>(1l));


    DataSet<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> otherGraphs =
      otherCollection.subgraphs
        .map(new Tuple2LongMapper<Subgraph<Long, EPFlinkGraphData>>(2l));

    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      thisGraphs.union(otherGraphs)
        .groupBy(new SubgraphTupleKeySelector<Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<Subgraph<Long, EPFlinkGraphData>,
          Long>, Subgraph<Long, EPFlinkGraphData>>() {


          @Override
          public void reduce(
            Iterable<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> iterable,
            Collector<Subgraph<Long, EPFlinkGraphData>> collector) throws
            Exception {
            Iterator<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> it =
              iterable.iterator();
            Tuple2<Subgraph<Long, EPFlinkGraphData>, Long> subgraph = null;
            Boolean inOtherCollection = false;
            while (it.hasNext()) {
              subgraph = it.next();
              if (subgraph.f1.equals(2l)) {
                inOtherCollection = true;
              }
            }
            if (!inOtherCollection) {
              collector.collect(subgraph.f0);
            }
          }
        });

    DataSet<Vertex<Long, EPFlinkVertexData>> unionVertices =
      this.graph.getVertices().union(otherCollection.graph.getVertices())
        .distinct(new VertexKeySelector());

    DataSet<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>> verticesWithGraphs =
      unionVertices.flatMap(
        new FlatMapFunction<Vertex<Long, EPFlinkVertexData>,
          Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>() {


          @Override
          public void flatMap(Vertex<Long, EPFlinkVertexData> vertexData,
            Collector<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>
              collector) throws
            Exception {
            for (Long graph : vertexData.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(vertexData, graph));
            }
          }
        });
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      verticesWithGraphs.join(newSubgraphs).where(1).equalTo(0).with(
        new JoinFunction<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>,
          Subgraph<Long, EPFlinkGraphData>, Vertex<Long, EPFlinkVertexData>>() {


          @Override
          public Vertex<Long, EPFlinkVertexData> join(
            Tuple2<Vertex<Long, EPFlinkVertexData>, Long> vertexLongTuple2,
            Subgraph<Long, EPFlinkGraphData> subgraph) throws Exception {
            return vertexLongTuple2.f0;
          }
        });

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();


    edges = edges.join(vertices).where(new EdgeSourceSelector()).equalTo(0)
      .with(new EdgeJoinFunction()).join(vertices)
      .where(new EdgeTargetSelector()).equalTo(0).with(new EdgeJoinFunction());

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  public EPGraphCollection alternateDifference(
    EPGraphCollection otherCollection) throws Exception {

    DataSet<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> thisGraphs =
      this.subgraphs
        .map(new Tuple2LongMapper<Subgraph<Long, EPFlinkGraphData>>(1l));


    DataSet<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> otherGraphs =
      otherCollection.subgraphs
        .map(new Tuple2LongMapper<Subgraph<Long, EPFlinkGraphData>>(2l));

    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      thisGraphs.union(otherGraphs)
        .groupBy(new SubgraphTupleKeySelector<Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<Subgraph<Long, EPFlinkGraphData>,
          Long>, Subgraph<Long, EPFlinkGraphData>>() {

          @Override
          public void reduce(
            Iterable<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> iterable,
            Collector<Subgraph<Long, EPFlinkGraphData>> collector) throws
            Exception {
            Iterator<Tuple2<Subgraph<Long, EPFlinkGraphData>, Long>> it =
              iterable.iterator();
            Tuple2<Subgraph<Long, EPFlinkGraphData>, Long> subgraph = null;
            Boolean inOtherCollection = false;
            while (it.hasNext()) {
              subgraph = it.next();
              if (subgraph.f1.equals(2l)) {
                inOtherCollection = true;
              }
            }
            if (!inOtherCollection) {
              collector.collect(subgraph.f0);
            }
          }
        });

    final List<Long> identifiers = newSubgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, Long>() {
        @Override
        public Long map(Subgraph<Long, EPFlinkGraphData> subgraph) throws
          Exception {
          return subgraph.getId();
        }
      }).collect();

    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices();
    vertices =
      vertices.filter(new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {

        @Override
        public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          for (Long id : identifiers) {
            if (vertex.getValue().getGraphs().contains(id)) {
              return true;
            }
          }
          return false;
        }
      });

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();


    edges = edges.join(vertices).where(new EdgeSourceSelector()).equalTo(0)
      .with(new EdgeJoinFunction()).join(vertices)
      .where(new EdgeTargetSelector()).equalTo(0).with(new EdgeJoinFunction());

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }


  @Override
  public EPGraphCollection distinct() {
    return null;
  }

  @Override
  public EPGraphCollection sortBy(String propertyKey, Order order) {
    return null;
  }

  @Override
  public EPGraphCollection top(int limit) {
    return null;
  }

  @Override
  public EPGraphCollection apply(UnaryFunction unaryFunction) {
    return null;
  }

  @Override
  public EPGraph reduce(BinaryFunction binaryGraphOperator) {
    return null;
  }

  @Override
  public EPGraph callForGraph(Algorithm algorithm, String... params) {
    return null;
  }

  @Override
  public EPGraphCollection callForCollection(Algorithm algorithm,
    String... params) {
    return null;
  }

  @Override
  public <V> Iterable<V> values(Class<V> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public Collection<EPGraphData> collect() throws Exception {
    return this.subgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, EPGraphData>() {
        @Override
        public EPFlinkGraphData map(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return longEPFlinkGraphDataSubgraph.getValue();
        }
      }).collect();
  }

  @Override
  public long size() throws Exception {
    return subgraphs.count();
  }

  @Override
  public void print() throws Exception {
    subgraphs.print();
  }

  EPGraph getGraph() {
    return EPGraph.fromGraph(this.graph, null, env);
  }

  /**
   * Used for distinction of vertices based on their unique id.
   */
  private static class SubgraphKeySelector implements
    KeySelector<Subgraph<Long, EPFlinkGraphData>, Long> {

    @Override
    public Long getKey(Subgraph<Long, EPFlinkGraphData> subgraph) throws
      Exception {
      return subgraph.getId();
    }
  }

  /**
   * Used for distinction of vertices based on their unique id.
   */
  private static class VertexKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    @Override
    public Long getKey(Vertex<Long, EPFlinkVertexData> vertex) throws
      Exception {
      return vertex.f0;
    }
  }

  /**
   * Used for distinction of edges based on their unique id.
   */
  private static class EdgeKeySelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(
      Edge<Long, EPFlinkEdgeData> longEPFlinkEdgeDataEdge) throws Exception {
      return longEPFlinkEdgeDataEdge.f2.getId();
    }
  }

  /**
   * Used for distinction of edges based on their source.
   */
  private static class EdgeSourceSelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(Edge<Long, EPFlinkEdgeData> edge) throws Exception {
      return edge.f0;
    }
  }

  /**
   * Used for distinction of edges based on their target.
   */
  private static class EdgeTargetSelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(Edge<Long, EPFlinkEdgeData> edge) throws Exception {
      return edge.f1;
    }
  }

  private static class SubgraphGroupReducer implements
    GroupReduceFunction<Subgraph<Long, EPFlinkGraphData>, Subgraph<Long,
      EPFlinkGraphData>> {

    /**
     * number of times a vertex must occur inside a group
     */
    private long amount;

    public SubgraphGroupReducer(long amount) {
      this.amount = amount;
    }

    @Override
    public void reduce(Iterable<Subgraph<Long, EPFlinkGraphData>> iterable,
      Collector<Subgraph<Long, EPFlinkGraphData>> collector) throws Exception {
      Iterator<Subgraph<Long, EPFlinkGraphData>> iterator = iterable.iterator();
      long count = 0L;
      Subgraph<Long, EPFlinkGraphData> s = null;
      while (iterator.hasNext()) {
        s = iterator.next();
        count++;
      }
      if (count == amount) {
        collector.collect(s);
      }
    }
  }

  private static class Tuple2LongMapper<C> implements
    MapFunction<C, Tuple2<C, Long>> {

    private Long secondField;

    public Tuple2LongMapper(Long secondField) {
      this.secondField = secondField;
    }

    @Override
    public Tuple2<C, Long> map(C c) throws Exception {
      return new Tuple2(c, secondField);
    }
  }

  private static class SubgraphTupleKeySelector<C> implements
    KeySelector<Tuple2<Subgraph<Long, EPFlinkGraphData>, C>, Long> {
    @Override
    public Long getKey(
      Tuple2<Subgraph<Long, EPFlinkGraphData>, C> subgraph) throws Exception {
      return subgraph.f0.getId();
    }
  }


  public static class EdgeJoinFunction implements
    JoinFunction<Edge<Long, EPFlinkEdgeData>, Vertex<Long,
      EPFlinkVertexData>, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public Edge<Long, EPFlinkEdgeData> join(
      Edge<Long, EPFlinkEdgeData> leftTuple,
      Vertex<Long, EPFlinkVertexData> rightTuple) throws Exception {
      return leftTuple;
    }
  }
}
