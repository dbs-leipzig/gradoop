package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Todo: Description and custom key selector
 */
public class SplitBy implements UnaryGraphToCollectionOperator, Serializable {
  /**
   * Flink Execution Enviroment
   */
  ExecutionEnvironment env;
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
  public SplitBy(String propertyKey, final ExecutionEnvironment env) {
    this.env = env;
    this.propertyKey = propertyKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph) {

    final String propKey = this.propertyKey;

    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> propertySelector =
      new KeySelector<Vertex<Long, EPFlinkVertexData>, Long>() {
        @Override
        public Long getKey(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          return (long) vertex.getValue().getProperties().get(propKey);
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
            (Long) vertex.getValue().getProperties().get(propKey);
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
              (Long) vertex.getValue().getProperties().get(propKey);
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

    DataSet<Set<Long>> newSubgraphIdentifiers = subgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, Set<Long>>() {
          @Override
          public Set<Long> map(Subgraph<Long, EPFlinkGraphData> subgraph) throws
            Exception {
            Set<Long> id = new HashSet<Long>();
            id.add(subgraph.getId());
            return id;
          }
        }).reduce(new ReduceFunction<Set<Long>>() {
        @Override
        public Set<Long> reduce(Set<Long> set1, Set<Long> set2) throws
          Exception {
          set1.addAll(set2);
          return set1;
        }
      });

    DataSet<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>> edgesWithSubgraphs =
      edgeGraphsGraphs.crossWithTiny(newSubgraphIdentifiers).with(
        new CrossFunction<Tuple3<Long, Set<Long>, Set<Long>>, Set<Long>,
          Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>>() {


          @Override
          public Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> cross(
            Tuple3<Long, Set<Long>, Set<Long>> tuple3,
            Set<Long> subgraphs) throws Exception {
            return new Tuple4<>(tuple3.f0, tuple3.f1, tuple3.f2, subgraphs);
          }
        });

    DataSet<Tuple4<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>,
      Set<Long>>>
      edgesWithGraphs =
      graph.getEdges().join(edgesWithSubgraphs).where(new EdgeKeySelector())
        .equalTo(0).with(
        new JoinFunction<Edge<Long, EPFlinkEdgeData>, Tuple4<Long, Set<Long>,
          Set<Long>, Set<Long>>, Tuple4<Edge<Long, EPFlinkEdgeData>,
          Set<Long>, Set<Long>, Set<Long>>>() {
          @Override
          public Tuple4<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>,
            Set<Long>> join(
            Edge<Long, EPFlinkEdgeData> edge,
            Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> tuple4) throws
            Exception {
            return new Tuple4<>(edge, tuple4.f1, tuple4.f2, tuple4.f3);
          }
        }

      );

    DataSet<Edge<Long, EPFlinkEdgeData>> edges = edgesWithGraphs.flatMap(
      new FlatMapFunction<Tuple4<Edge<Long, EPFlinkEdgeData>, Set<Long>,
        Set<Long>, Set<Long>>, Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public void flatMap(
          Tuple4<Edge<Long, EPFlinkEdgeData>, Set<Long>, Set<Long>,
            Set<Long>> tuple4,
          Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
          Edge<Long, EPFlinkEdgeData> edge = tuple4.f0;
          Set<Long> sourceGraphs = tuple4.f1;
          Set<Long> targetGraphs = tuple4.f2;
          Set<Long> newSubgraphs = tuple4.f3;
          boolean newGraphAdded = false;
          for (Long graph : newSubgraphs) {
            if (targetGraphs.contains(graph) && sourceGraphs.contains(graph)) {
              edge.getValue().getGraphs().add(graph);
              newGraphAdded = true;
            }
          }
          if (newGraphAdded) {
            collector.collect(edge);
          }
        }
      });

    try {
      edges.print();
      System.out.println("fdsa");
    } catch (Exception e) {
      e.printStackTrace();
    }

    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);
    return new

      EPGraphCollection(newGraph, subgraphs, env);
  }

  @Override
  public String getName() {
    return null;
  }

  /**
   * Used for distinction of edges based on their unique id.
   */
  private static class EdgeKeySelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(
      Edge<Long, EPFlinkEdgeData> longEPFlinkEdgeDataEdge) throws Exception {
      return longEPFlinkEdgeDataEdge.getValue().getId();
    }
  }
}
