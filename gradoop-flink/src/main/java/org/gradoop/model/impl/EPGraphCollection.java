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

import com.google.common.collect.Lists;
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
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.operators.EPGraphCollectionOperators;
import org.gradoop.model.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.gradoop.model.impl.EPGraph.*;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 *
 * @author Martin Junghanns
 * @author Niklas Teichmann
 */
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
      .subgraph(new VertexGraphContainmentFilter(graphID),
        new EdgeGraphContainmentFilter(graphID));

    DataSet<Tuple1<Long>> graphIDDataSet =
      env.fromCollection(Lists.newArrayList(new Tuple1<>(graphID)));

    // get graph data based on graph id
    EPFlinkGraphData graphData =
      this.subgraphs.joinWithTiny(graphIDDataSet).where(GRAPH_ID).equalTo(0)
        .with(
          new JoinFunction<Subgraph<Long, EPFlinkGraphData>, Tuple1<Long>,
            EPFlinkGraphData>() {
            @Override
            public EPFlinkGraphData join(Subgraph<Long, EPFlinkGraphData> g,
              Tuple1<Long> gID) throws Exception {
              return g.getValue();
            }
          }).first(1).collect().get(0);
    return EPGraph.fromGraph(subGraph, graphData);
  }

  @Override
  public EPGraphCollection getGraphs(final Long... identifiers) throws
    Exception {
    return getGraphs(Arrays.asList(identifiers));
  }

  public EPGraphCollection getGraphs(final List<Long> identifiers) throws
    Exception {

    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubGraphs = this.subgraphs
      .filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {

        @Override
        public boolean filter(Subgraph<Long, EPFlinkGraphData> subgraph) throws
          Exception {
          return identifiers.contains(subgraph.getId());

        }
      });

    // build new vertex set
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices().filter(new VertexInGraphFilter(identifiers));

    // build new edge set
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      this.graph.getEdges().filter(new EdgeInGraphFilter(identifiers));

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubGraphs, env);
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
        public boolean filter(Subgraph<Long, EPFlinkGraphData> g) throws
          Exception {
          return predicateFunction.filter(g.getValue());
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
      this.subgraphs.union(otherCollection.subgraphs).distinct(GRAPH_ID);
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices().union(otherCollection.graph.getVertices())
        .distinct(VERTEX_ID);
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      this.graph.getEdges().union(otherCollection.graph.getEdges())
        .distinct(EDGE_ID);

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, env);
  }

  @Override
  public EPGraphCollection intersect(EPGraphCollection otherCollection) throws
    Exception {
    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      this.subgraphs.union(otherCollection.subgraphs).groupBy(GRAPH_ID)
        .reduceGroup(new SubgraphGroupReducer(2));

    DataSet<Vertex<Long, EPFlinkVertexData>> thisVertices =
      this.graph.getVertices();

    DataSet<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>> verticesWithGraphs =
      thisVertices.flatMap(
        new FlatMapFunction<Vertex<Long, EPFlinkVertexData>,
          Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>() {

          @Override
          public void flatMap(Vertex<Long, EPFlinkVertexData> v,
            Collector<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>>
              collector) throws
            Exception {
            for (Long graph : v.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(v, graph));
            }
          }
        });

    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      verticesWithGraphs.join(newSubgraphs).where(1).equalTo(GRAPH_ID).with(
        new JoinFunction<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>,
          Subgraph<Long, EPFlinkGraphData>, Vertex<Long, EPFlinkVertexData>>() {

          @Override
          public Vertex<Long, EPFlinkVertexData> join(
            Tuple2<Vertex<Long, EPFlinkVertexData>, Long> vertices,
            Subgraph<Long, EPFlinkGraphData> subgraph) throws Exception {
            return vertices.f0;
          }
        });

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();

    edges = edges.join(vertices).where(SOURCE_VERTEX_ID).equalTo(VERTEX_ID)
      .with(new EdgeJoinFunction()).join(vertices).where(TARGET_VERTEX_ID)
      .equalTo(VERTEX_ID).with(new EdgeJoinFunction());

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, env);
  }

  public EPGraphCollection alternateIntersect(
    EPGraphCollection otherCollection) throws Exception {

    final DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      this.subgraphs.union(otherCollection.subgraphs).groupBy(GRAPH_ID)
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

    edges = edges.join(vertices).where(SOURCE_VERTEX_ID).equalTo(VERTEX_ID)
      .with(new EdgeJoinFunction()).join(vertices).where(TARGET_VERTEX_ID)
      .equalTo(VERTEX_ID).with(new EdgeJoinFunction());

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

    DataSet<Vertex<Long, EPFlinkVertexData>> thisVertices =
      this.graph.getVertices().union(otherCollection.graph.getVertices())
        .distinct(VERTEX_ID);

    DataSet<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>> verticesWithGraphs =
      thisVertices.flatMap(
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
      verticesWithGraphs.join(newSubgraphs).where(1).equalTo(GRAPH_ID).with(
        new JoinFunction<Tuple2<Vertex<Long, EPFlinkVertexData>, Long>,
          Subgraph<Long, EPFlinkGraphData>, Vertex<Long, EPFlinkVertexData>>() {

          @Override
          public Vertex<Long, EPFlinkVertexData> join(
            Tuple2<Vertex<Long, EPFlinkVertexData>, Long> vertexLongTuple,
            Subgraph<Long, EPFlinkGraphData> subgraph) throws Exception {
            return vertexLongTuple.f0;
          }
        }).distinct(VERTEX_ID);


    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();

    edges = edges.join(vertices).where(SOURCE_VERTEX_ID).equalTo(VERTEX_ID)
      .with(new EdgeJoinFunction()).join(vertices).where(TARGET_VERTEX_ID)
      .equalTo(VERTEX_ID).with(new EdgeJoinFunction()).distinct(EDGE_ID);

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, env);
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
          boolean vertexInGraph = false;
          for (Long id : identifiers) {
            if (vertex.getValue().getGraphs().contains(id)) {
              vertexInGraph = true;
              break;
            }
          }
          return vertexInGraph;
        }
      });

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = this.graph.getEdges();

    edges = edges.join(vertices).where(SOURCE_VERTEX_ID).equalTo(VERTEX_ID)
      .with(new EdgeJoinFunction()).join(vertices).where(TARGET_VERTEX_ID)
      .equalTo(VERTEX_ID).with(new EdgeJoinFunction());

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, env);
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
  public EPGraphCollection apply(UnaryGraphToGraphOperator op) {
    return null;
  }

  @Override
  public EPGraph reduce(BinaryGraphToGraphOperator op) {
    return null;
  }

  @Override
  public EPGraphCollection callForCollection(
    UnaryCollectionToCollectionOperator op) {
    return op.execute(this);
  }

  @Override
  public EPGraphCollection callForCollection(
    BinaryCollectionToCollectionOperator op,
    EPGraphCollection otherCollection) {
    return op.execute(this, otherCollection);
  }

  @Override
  public EPGraph callForGraph(UnaryCollectionToGraphOperator op) {
    return op.execute(this);
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
    return EPGraph.fromGraph(this.graph, null);
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
      return new Tuple2<>(c, secondField);
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

  private static class VertexGraphContainmentFilter implements
    FilterFunction<Vertex<Long, EPFlinkVertexData>> {

    private long graphID;

    public VertexGraphContainmentFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Vertex<Long, EPFlinkVertexData> v) throws Exception {
      return v.f1.getGraphs().contains(graphID);
    }
  }

  private static class EdgeGraphContainmentFilter implements
    FilterFunction<Edge<Long, EPFlinkEdgeData>> {

    private long graphID;

    public EdgeGraphContainmentFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Edge<Long, EPFlinkEdgeData> e) throws Exception {
      return e.f2.getGraphs().contains(graphID);
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


  public static class VertexInGraphFilter implements
    FilterFunction<Vertex<Long, EPFlinkVertexData>> {

    List<Long> identifiers;

    public VertexInGraphFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
      Exception {
      boolean vertexInGraph = false;
      for (Long graph : vertex.getValue().getGraphs()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
      return vertexInGraph;
    }
  }

  public static class EdgeInGraphFilter implements
    FilterFunction<Edge<Long, EPFlinkEdgeData>> {

    List<Long> identifiers;

    public EdgeInGraphFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Edge<Long, EPFlinkEdgeData> vertex) throws Exception {
      boolean vertexInGraph = false;
      for (Long graph : vertex.getValue().getGraphs()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
      return vertexInGraph;
    }
  }
}
