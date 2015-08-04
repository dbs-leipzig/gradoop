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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.helper.LongFromVertexFunction;
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
 * operator used to split an EPGraph into a EPGraphCollection of graphs with
 * distinct vertices, uses a LongFromVertexFunction to define the groups of
 * vertices that form the new graphs
 */
public class SplitBy implements UnaryGraphToCollectionOperator, Serializable {
  /**
   * Flink execution enviroment
   */
  ExecutionEnvironment env;
  /**
   * function, that defines a distinct mapping vertex -> long on the graph
   */
  final LongFromVertexFunction function;

  /**
   * public constructor
   *
   * @param env      ExecutionEnvironment
   * @param function LongFromVertexFunction
   */
  public SplitBy(LongFromVertexFunction function,
    final ExecutionEnvironment env) {
    this.env = env;
    this.function = function;
  }

  /**
   * {@inheritDoc}
   * executes the SplitBy operation on a graph
   *
   * @param epGraph EPGraph
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph) {
    // construct a KeySelector using the LongFromVertexFunction
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> propertySelector =
      new LongFromVertexSelector(function);
    //get the Gelly graph and vertices
    final Graph graph = epGraph.getGellyGraph();
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices = graph.getVertices();
    //add the new graphs to the vertices graph lists
    vertices = vertices.map(new AddNewGraphsToVertexMapper(function));
    //construct the list of subgraphs
    DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs =
      vertices.groupBy(propertySelector)
        .reduceGroup(new SubgraphsFromGroupsReducer(function));
    //construct tuples of the edges with the ids of their source and target
    // vertices
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
    //replace the source vertex id by the graph list of this vertex
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
    //replace the target vertex id by the graph list of this vertex
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
    //transform the new subgraphs into a single set of long, containing all
    // the identifiers
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
    //construct new tuples containing the edge, the graphs of its source and
    //target vertex and the list of new graphs
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
    //remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<Long, Set<Long>>> newSubgraphs = edgesWithSubgraphs.flatMap(
      new FlatMapFunction<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>,
        Tuple2<Long, Set<Long>>>() {
        @Override
        public void flatMap(
          Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> tuple4,
          Collector<Tuple2<Long, Set<Long>>> collector) throws Exception {
          Set<Long> sourceGraphs = tuple4.f1;
          Set<Long> targetGraphs = tuple4.f2;
          Set<Long> newSubgraphs = tuple4.f3;
          boolean newGraphAdded = false;
          Set<Long> toBeAddedGraphs = new HashSet<Long>();
          for (Long graph : newSubgraphs) {
            if (targetGraphs.contains(graph) && sourceGraphs.contains(graph)) {
              toBeAddedGraphs.add(graph);
              newGraphAdded = true;
            }
          }
          if (newGraphAdded) {
            collector.collect(new Tuple2<>(tuple4.f0, toBeAddedGraphs));
          }
        }
      });
    //join the graph set tuples with the edges, add all new graphs to the
    //edge graph sets
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      graph.getEdges().join(newSubgraphs).where(new EdgeKeySelector())
        .equalTo(0).with(
        new JoinFunction<Edge<Long, EPFlinkEdgeData>, Tuple2<Long,
          Set<Long>>, Edge<Long, EPFlinkEdgeData>>() {
          @Override
          public Edge<Long, EPFlinkEdgeData> join(
            Edge<Long, EPFlinkEdgeData> edge,
            Tuple2<Long, Set<Long>> tuple2) throws Exception {
            return edge;
          }
        });
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);
    return new EPGraphCollection(newGraph, subgraphs, env);
  }

  @Override
  public String getName() {
    return "SplitBy";
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

  /**
   * applies the LongFromVertexFunction on a vertex
   */
  private static class LongFromVertexSelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    LongFromVertexFunction function;

    public LongFromVertexSelector(LongFromVertexFunction function) {
      this.function = function;
    }

    @Override
    public Long getKey(Vertex<Long, EPFlinkVertexData> vertex) throws
      Exception {
      return function.extractLong(vertex);
    }
  }

  /**
   * adds the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set
   */
  private static class AddNewGraphsToVertexMapper implements
    MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
      EPFlinkVertexData>> {
    private LongFromVertexFunction function;

    public AddNewGraphsToVertexMapper(LongFromVertexFunction function) {
      this.function = function;
    }

    @Override
    public Vertex<Long, EPFlinkVertexData> map(
      Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
      Long labelPropIndex = function.extractLong(vertex);
      vertex.getValue().getGraphs().add(labelPropIndex);
      return vertex;
    }
  }

  /**
   * builds new graphs from vertices and the LongFromVertexFunction
   */
  private static class SubgraphsFromGroupsReducer implements
    GroupReduceFunction<Vertex<Long, EPFlinkVertexData>, Subgraph<Long,
      EPFlinkGraphData>> {
    private LongFromVertexFunction function;

    public SubgraphsFromGroupsReducer(LongFromVertexFunction function) {
      this.function = function;
    }

    @Override
    public void reduce(Iterable<Vertex<Long, EPFlinkVertexData>> iterable,
      Collector<Subgraph<Long, EPFlinkGraphData>> collector) throws Exception {
      Iterator<Vertex<Long, EPFlinkVertexData>> it = iterable.iterator();
      Vertex<Long, EPFlinkVertexData> vertex = it.next();
      Long labelPropIndex = function.extractLong(vertex);
      EPFlinkGraphData subgraphData =
        new EPFlinkGraphData(labelPropIndex, "split graph " + labelPropIndex);
      Subgraph<Long, EPFlinkGraphData> newSubgraph =
        new Subgraph(labelPropIndex, subgraphData);
      collector.collect(newSubgraph);
    }
  }
}
