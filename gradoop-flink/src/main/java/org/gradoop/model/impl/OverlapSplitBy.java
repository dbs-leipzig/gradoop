package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.helper.LongListFromVertexFunction;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Split an EPGraph into an EPGraphCollection by a self defined mapping from
 * vertex to long
 */
public class OverlapSplitBy implements UnaryGraphToCollectionOperator,
  Serializable {
  ExecutionEnvironment env;
  final LongListFromVertexFunction function;

  public OverlapSplitBy(LongListFromVertexFunction function,
    ExecutionEnvironment env) {
    this.env = env;
    this.function = function;
  }

  /**
   * execute the operator, split the EPGraph into an EPGraphCollection which
   * graphs can be overlapping
   *
   * @param epGraph the epGraph that will be split
   * @return a GraphCollection containing the newly created EPGraphs
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph) {
    final Graph graph = epGraph.getGellyGraph();
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices = graph.getVertices();
    //add all new subgraphs to the graph sets of the vertices
    vertices = vertices.map(new AddNewGraphsToVertexMapper(function));
    //extract the distinct ids of the new subgraphs
    DataSet<Tuple1<Long>> newSubgraphIDs =
      vertices.flatMap(new VertexToGraphIDFlatMapper(function)).distinct();
    //construct the new subgraph objects
    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      newSubgraphIDs.map(new SubgraphMapper());
    //construct tuples containing the edge, source and target vertex ids
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      graph.getEdges().map(new EdgeSourceTargetMapper());
    //replace the source vertex id by the graph set of this vertex
    DataSet<Tuple3<Long, List<Long>, Long>> edgeGraphsVertex =
      edgeVertexVertex.join(vertices).where(1).equalTo(0)
        .with(new EdgesSourceGraphSetJoin());
    //replace the target vertex id by the graph set of this vertex
    DataSet<Tuple3<Long, List<Long>, List<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex.join(vertices).where(2).equalTo(0)
        .with(new EdgesTargetGraphSetJoin());
    //transform the new subgraphs into a single set of long, containing all
    // the identifiers
    DataSet<List<Long>> newSubgraphIdentifiers =
      newSubgraphs.map(new SubgraphsToLongSetMapper())
        .reduce(new SubgraphLongSetReducer());
    //construct new tuples containing the edge, the graphs of its source and
    //target vertex and the list of new graphs
    DataSet<Tuple4<Long, List<Long>, List<Long>, List<Long>>>
      edgesWithSubgraphs =
      edgeGraphsGraphs.crossWithTiny(newSubgraphIdentifiers)
        .with(new CrossEdgeWithSubgraphSet());
    //remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<Long, List<Long>>> subgraphs =
      edgesWithSubgraphs.flatMap(new EdgesInSubgraphsFlatMapper());
    //join the graph set tuples with the edges, add all new graphs to the
    //edge graph sets
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      graph.getEdges().join(subgraphs).where(new EdgeKeySelector()).equalTo(0)
        .with(new EdgeTuplesWithEdgesJoin());
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);
    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  /**
   * add the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set
   */
  private static class AddNewGraphsToVertexMapper implements
    MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
      EPFlinkVertexData>> {
    private LongListFromVertexFunction function;

    public AddNewGraphsToVertexMapper(LongListFromVertexFunction function) {
      this.function = function;
    }

    @Override
    public Vertex<Long, EPFlinkVertexData> map(
      Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
      List<Long> labelPropIndex = function.extractLongList(vertex);
      vertex.getValue().getGraphs().addAll(labelPropIndex);
      return vertex;
    }
  }

  /**
   * maps the vertices to tuples containing all the graph ids
   */
  private static class VertexToGraphIDFlatMapper implements
    FlatMapFunction<Vertex<Long, EPFlinkVertexData>, Tuple1<Long>> {
    LongListFromVertexFunction function;

    public VertexToGraphIDFlatMapper(LongListFromVertexFunction function) {
      this.function = function;
    }

    @Override
    public void flatMap(Vertex<Long, EPFlinkVertexData> vertex,
      Collector<Tuple1<Long>> collector) throws Exception {
      List<Long> graphIDSet = function.extractLongList(vertex);
      for (Long id : graphIDSet) {
        collector.collect(new Tuple1<>(id));
      }
    }
  }

  /**
   * map the graph ids to subraphs
   */
  private static class SubgraphMapper implements
    MapFunction<Tuple1<Long>, Subgraph<Long, EPFlinkGraphData>> {
    @Override
    public Subgraph<Long, EPFlinkGraphData> map(Tuple1<Long> idTuple) throws
      Exception {
      Long id = idTuple.f0;
      EPFlinkGraphData subgraphData =
        new EPFlinkGraphData(id, "split graph " + id);
      return new Subgraph<>(id, subgraphData);
    }
  }

  /**
   * used for distinction of edges based on their unique id.
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
   * transform an edge into a Tuple3 of edge id, source vertex and
   * target id
   */
  private static class EdgeSourceTargetMapper implements
    MapFunction<Edge<Long, EPFlinkEdgeData>, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(Edge<Long, EPFlinkEdgeData> edge) throws
      Exception {
      return new Tuple3<>(edge.getValue().getId(),
        edge.getValue().getSourceVertex(), edge.getValue().getTargetVertex());
    }
  }

  /**
   * join edge tuples with the graph sets of their sources
   */
  private static class EdgesSourceGraphSetJoin implements
    JoinFunction<Tuple3<Long, Long, Long>, Vertex<Long, EPFlinkVertexData>,
      Tuple3<Long, List<Long>, Long>> {
    @Override
    public Tuple3<Long, List<Long>, Long> join(Tuple3<Long, Long, Long> tuple3,
      Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
      return new Tuple3<>(tuple3.f0,
        (List<Long>) Lists.newArrayList(vertex.getValue().getGraphs()),
        tuple3.f2);
    }
  }

  /**
   * join edge tuples with the graph sets of their targets
   */
  private static class EdgesTargetGraphSetJoin implements
    JoinFunction<Tuple3<Long, List<Long>, Long>, Vertex<Long,
      EPFlinkVertexData>, Tuple3<Long, List<Long>, List<Long>>> {
    @Override
    public Tuple3<Long, List<Long>, List<Long>> join(
      Tuple3<Long, List<Long>, Long> tuple3,
      Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
      return new Tuple3<>(tuple3.f0, tuple3.f1,
        (List<Long>) Lists.newArrayList(vertex.getValue().getGraphs()));
    }
  }

  /**
   * map a subgraph to a set of longs, containing the identifier of
   * the subgraph
   */
  private static class SubgraphsToLongSetMapper implements
    MapFunction<Subgraph<Long, EPFlinkGraphData>, List<Long>> {
    @Override
    public List<Long> map(Subgraph<Long, EPFlinkGraphData> subgraph) throws
      Exception {
      List<Long> id = new ArrayList<Long>();
      id.add(subgraph.getId());
      return id;
    }
  }

  /**
   * reduce a dataset of sets of longs into a single set of longs
   */
  private static class SubgraphLongSetReducer implements
    ReduceFunction<List<Long>> {
    @Override
    public List<Long> reduce(List<Long> set1, List<Long> set2) throws
      Exception {
      set1.addAll(set2);
      return set1;
    }
  }

  /**
   * add the set of subgraphs to the edge tuples
   */
  private static class CrossEdgeWithSubgraphSet implements
    CrossFunction<Tuple3<Long, List<Long>, List<Long>>, List<Long>,
      Tuple4<Long, List<Long>, List<Long>, List<Long>>> {
    @Override
    public Tuple4<Long, List<Long>, List<Long>, List<Long>> cross(
      Tuple3<Long, List<Long>, List<Long>> tuple3, List<Long> subgraphs) throws
      Exception {
      return new Tuple4<>(tuple3.f0, tuple3.f1, tuple3.f2, subgraphs);
    }
  }

  /**
   * check if the source and target vertices of the edges are in the
   * same new subgraphs and to update the edgesList
   */
  private static class EdgesInSubgraphsFlatMapper implements
    FlatMapFunction<Tuple4<Long, List<Long>, List<Long>, List<Long>>,
      Tuple2<Long, List<Long>>> {
    @Override
    public void flatMap(Tuple4<Long, List<Long>, List<Long>, List<Long>> tuple4,
      Collector<Tuple2<Long, List<Long>>> collector) throws Exception {
      List<Long> sourceGraphs = tuple4.f1;
      List<Long> targetGraphs = tuple4.f2;
      List<Long> newSubgraphs = tuple4.f3;
      boolean newGraphAdded = false;
      List<Long> toBeAddedGraphs = new ArrayList<Long>();
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
  }

  /**
   * join the edge tuples with the actual edges
   */
  private static class EdgeTuplesWithEdgesJoin implements
    JoinFunction<Edge<Long, EPFlinkEdgeData>, Tuple2<Long, List<Long>>,
      Edge<Long, EPFlinkEdgeData>> {
    @Override
    public Edge<Long, EPFlinkEdgeData> join(Edge<Long, EPFlinkEdgeData> edge,
      Tuple2<Long, List<Long>> tuple2) throws Exception {
      return edge;
    }
  }

  @Override
  public String getName() {
    return "OverlapSplitBy";
  }
}
