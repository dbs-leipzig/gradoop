package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.util.Iterator;
import java.util.Set;

/**
 * Todo: Description
 */
public class CallByPropertyKey implements UnaryGraphToCollectionOperator {
  /**
   * Flink Execution Enviroment
   */
  final ExecutionEnvironment env;
  /**
   * EPGM Propertykey
   */
  final String propertyKey;

  /**
   * Public Constructor
   *
   * @param propertyKey String propertyKey
   * @param env         ExecutionEnvironment
   */
  public CallByPropertyKey(String propertyKey, final ExecutionEnvironment env) {
    this.env = env;
    this.propertyKey = propertyKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph){
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> propertySelector =
      new KeySelector<Vertex<Long, EPFlinkVertexData>, Long>() {
        @Override
        public Long getKey(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          return (long) vertex.getValue().getProperties().get(propertyKey);
        }
      };
    Graph graph = epGraph.getGellyGraph();
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices = graph.getVertices();
    vertices = vertices.map(
      new MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
        EPFlinkVertexData>>() {
        @Override
        public Vertex<Long, EPFlinkVertexData> map(
          Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
          Long labelPropIndex =
            (Long) vertex.getValue().getProperties().get(propertyKey);
          vertex.getValue().getGraphs().add(labelPropIndex);
          return vertex;
        }
      });
    DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs =
      vertices.groupBy(propertySelector).reduceGroup(
        new GroupReduceFunction<Vertex<Long, EPFlinkVertexData>,
          Subgraph<Long, EPFlinkGraphData>>() {
          @Override
          public void reduce(Iterable<Vertex<Long, EPFlinkVertexData>> iterable,
            Collector<Subgraph<Long, EPFlinkGraphData>> collector) throws
            Exception {
            Iterator<Vertex<Long, EPFlinkVertexData>> it = iterable.iterator();
            Vertex<Long, EPFlinkVertexData> vertex = it.next();
            Long labelPropIndex =
              (Long) vertex.getValue().getProperties().get(propertyKey);
            EPFlinkGraphData subgraphData = new EPFlinkGraphData(labelPropIndex,
              "propagation graph " + labelPropIndex);
            Subgraph<Long, EPFlinkGraphData> newSubgraph =
              new Subgraph(labelPropIndex, subgraphData);
            collector.collect(newSubgraph);
          }
        });
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex = graph.getEdges().map(
      new MapFunction<Edge<Long, EPFlinkEdgeData>, Tuple3<Long, Long, Long>>() {
        @Override
        public Tuple3<Long, Long, Long> map(
          Edge<Long, EPFlinkEdgeData> edge) throws Exception {
          return new Tuple3<>(edge.getValue().getId(),
            edge.getValue().getSourceVertex(),
            edge.getValue().getTargetVertex());
        }
      });
    DataSet<Tuple3<Long, Set<Long>, Long>> edgeGraphsVertex =
      edgeVertexVertex.join(vertices).where(1).equalTo(0).with(
        new JoinFunction<Tuple3<Long, Long, Long>, Vertex<Long,
          EPFlinkVertexData>, Tuple3<Long, Set<Long>, Long>>() {
          @Override
          public Tuple3<Long, Set<Long>, Long> join(
            Tuple3<Long, Long, Long> tuple3,
            Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
            return new Tuple3<>(tuple3.f0, vertex.getValue().getGraphs(),
              tuple3.f2);
          }
        });
    DataSet<Tuple3<Long, Set<Long>, Set<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex.join(vertices).where(2).equalTo(0).with(
        new JoinFunction<Tuple3<Long, Set<Long>, Long>, Vertex<Long,
          EPFlinkVertexData>, Tuple3<Long, Set<Long>, Set<Long>>>() {
          @Override
          public Tuple3<Long, Set<Long>, Set<Long>> join(
            Tuple3<Long, Set<Long>, Long> tuple3,
            Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
            return new Tuple3<>(tuple3.f0, tuple3.f1,
              vertex.getValue().getGraphs());
          }
        });
    DataSet<Tuple3<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>>>
      edgesWithGraphs =
      graph.getEdges().join(edgeGraphsGraphs).where(0).equalTo(0).with(
        new JoinFunction<Edge<Long, EPFlinkEdgeData>, Tuple3<Long, Set<Long>,
          Set<Long>>, Tuple3<Edge<Long, EPFlinkEdgeData>, Set<Long>,
          Set<Long>>>() {
          @Override
          public Tuple3<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>> join(
            Edge<Long, EPFlinkEdgeData> edge,
            Tuple3<Long, Set<Long>, Set<Long>> tuple3) throws Exception {
            return new Tuple3<>(edge, tuple3.f1, tuple3.f2);
          }
        });
    DataSet<Edge<Long, EPFlinkEdgeData>> edges = edgesWithGraphs.flatMap(
      new FlatMapFunction<Tuple3<Edge<Long, EPFlinkEdgeData>, Set<Long>,
        Set<Long>>, Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public void flatMap(
          Tuple3<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>> tuple3,
          Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
          Edge<Long, EPFlinkEdgeData> edge = tuple3.f0;
          Set<Long> sourceGraphs = tuple3.f1;
          Set<Long> targetGraphs = tuple3.f2;
          boolean newGraphAdded = false;
          for (Long graph : sourceGraphs) {
            if (targetGraphs.contains(graph) &&
              !edge.getValue().getGraphs().contains(graph)) {
              edge.getValue().getGraphs().add(graph);
              newGraphAdded = true;
            }
          }
          if (newGraphAdded) {
            collector.collect(edge);
          }
        }
      });
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);
    return new EPGraphCollection(newGraph, subgraphs, env);
  }

  @Override
  public String getName() {
    return null;
  }
}
