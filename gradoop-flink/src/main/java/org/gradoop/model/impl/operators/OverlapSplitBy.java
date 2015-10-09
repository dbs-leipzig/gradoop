package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Split a LogicalGraph into an GraphCollection by a self defined mapping from
 * vertex to graph id.
 *
 * @param <VD> VertexData contains information about the vertex
 * @param <ED> EdgeData contains information about all edges of the vertex
 * @param <GD> GraphData contains information about all graphs of the vertex
 */
public class OverlapSplitBy<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD>, Serializable {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 513465233452L;
  /**
   * Flink Execution Environment
   */
  private final transient ExecutionEnvironment env;
  /**
   * Self defined function for graph extraction
   */
  private final UnaryFunction<VD, List<Long>> function;

  /**
   * Constructor
   *
   * @param function self defined function
   * @param env      execution environment
   */
  public OverlapSplitBy(UnaryFunction<VD, List<Long>> function,
    ExecutionEnvironment env) {
    this.env = env;
    this.function = function;
  }

  /**
   * execute the operator, split the EPGraph into an EPGraphCollection which
   * graphs can be overlapping
   *
   * @param logicalGraph the epGraph that will be split
   * @return a GraphCollection containing the newly created EPGraphs
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    // add all new subgraphs to the graph sets of the vertices
    DataSet<Vertex<Long, VD>> vertices = computeNewVertices(logicalGraph);
    // construct the new subgraph objects
    DataSet<Subgraph<Long, GD>> subgraphs =
      computeNewSubgraphs(logicalGraph, vertices);
    // construct tuples containing the edge, source and target vertex ids
    DataSet<Edge<Long, ED>> edges =
      computeNewEdges(logicalGraph, vertices, subgraphs);
    Graph<Long, VD, ED> newGraph = Graph.fromDataSet(vertices, edges, env);
    return new GraphCollection<>(newGraph, subgraphs,
      logicalGraph.getVertexDataFactory(), logicalGraph.getEdgeDataFactory(),
      logicalGraph.getGraphDataFactory(), env);
  }

  /**
   * compute the vertices in the new graphs created by the SplitBy and add
   * these graphs to the graph sets of the vertices
   *
   * @param logicalGraph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<Vertex<Long, VD>> computeNewVertices(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    // add the new graphs to the vertices graph lists
    return logicalGraph.getVertices()
      .map(new AddNewGraphsToVertexMapper<>(function));
  }

  /**
   * compute the new subgraphs created by the OverlapSplitBy
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<Subgraph<Long, GD>> computeNewSubgraphs(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices) {
    DataSet<Tuple1<Long>> newSubgraphIDs =
      vertices.flatMap(new VertexToGraphIDFlatMapper<>(function)).distinct();
    GraphDataFactory<GD> gdFactory = logicalGraph.getGraphDataFactory();
    return newSubgraphIDs.map(new SubgraphMapper<>(gdFactory));
  }

  /**
   * compute the new edges between all subgraphs
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their edges
   * @param subgraphs    the subgraphs of the vertices
   * @return all edges between all subgraphs
   */
  private DataSet<Edge<Long, ED>> computeNewEdges(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices,
    DataSet<Subgraph<Long, GD>> subgraphs) {
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      logicalGraph.getEdges().map(new EdgeToTupleMapper<ED>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, List<Long>, Long>> edgeGraphsVertex = edgeVertexVertex
        .join(vertices)
        .where(1)
        .equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, List<Long>, List<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex
        .join(vertices)
        .where(2)
        .equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    // transform the new subgraphs into a single set of long, containing all
    // the identifiers
    DataSet<List<Long>> newSubgraphIdentifiers = subgraphs
      .map(new MapSubgraphIdToSet<GD>())
      .reduce(new ReduceSets());
    // construct new tuples containing the edge, the graphs of its source and
    // target vertex and the list of new graphs
    DataSet<Tuple4<Long, List<Long>, List<Long>, List<Long>>>
      edgesWithSubgraphs = edgeGraphsGraphs
      .crossWithTiny(newSubgraphIdentifiers).with(new CrossEdgesWithGraphSet());
    // remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<Long, List<Long>>> newSubgraphs = edgesWithSubgraphs
      .flatMap(new CheckEdgesSourceTargetGraphs());
    // join the graph set tuples with the edges, add all new graphs to the
    // edge graph sets
    return logicalGraph.getEdges()
      .join(newSubgraphs)
      .where(new EdgeKeySelector<ED>())
      .equalTo(0)
      .with(new JoinEdgeTuplesWithEdges<ED>());
  }

  /**
   * map the graph ids to subgraphs
   */
  private static class SubgraphMapper<GD extends GraphData> implements
    MapFunction<Tuple1<Long>, Subgraph<Long, GD>>,
    ResultTypeQueryable<Subgraph<Long, GD>> {
    /**
     * GraphDataFactory
     */
    private GraphDataFactory<GD> graphDataFactory;

    /**
     * Constructor
     *
     * @param graphDataFactory actual GraphDataFactory
     */
    public SubgraphMapper(GraphDataFactory<GD> graphDataFactory) {
      this.graphDataFactory = graphDataFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subgraph<Long, GD> map(Tuple1<Long> idTuple) throws Exception {
      Long id = idTuple.f0;
      return new Subgraph<>(id,
        graphDataFactory.createGraphData(id, "split graph " + id));
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<Subgraph<Long, GD>> getProducedType() {
      return new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(graphDataFactory.getType()));
    }
  }

  /**
   * maps the vertices to tuple containing all the graph ids
   */
  private static class VertexToGraphIDFlatMapper<VD extends VertexData>
    implements
    FlatMapFunction<Vertex<Long, VD>, Tuple1<Long>> {
    /**
     * Self defined Function
     */
    private UnaryFunction<VD, List<Long>> function;

    /**
     * Constructor
     *
     * @param function actual defined Function
     */
    public VertexToGraphIDFlatMapper(
      UnaryFunction<VD, List<Long>> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(Vertex<Long, VD> vertex,
      Collector<Tuple1<Long>> collector) throws Exception {
      List<Long> graphIDSet = function.execute(vertex.getValue());
      for (Long id : graphIDSet) {
        collector.collect(new Tuple1<>(id));
      }
    }
  }

  /**
   * add the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set
   */
  private static class AddNewGraphsToVertexMapper<VD extends VertexData>
    implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {
    /**
     * Self defined Function
     */
    private UnaryFunction<VD, List<Long>> function;

    /**
     * Constructor
     *
     * @param function actual defined Function
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<VD, List<Long>> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> vertex) throws Exception {
      List<Long> labelPropIndex = function.execute(vertex.getValue());
      if (vertex.getValue().getGraphs() == null) {
        vertex.getValue().setGraphs(new HashSet<Long>());
      }
      vertex.getValue().getGraphs().addAll(labelPropIndex);
      return vertex;
    }
  }

  /**
   * transform an edge into a Tuple3 of edge id, source vertex and
   * target id
   */
  private static class EdgeToTupleMapper<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(Edge<Long, ED> edge) throws Exception {
      return new Tuple3<>(edge.getValue().getId(),
        edge.getValue().getSourceVertexId(),
        edge.getValue().getTargetVertexId());
    }
  }

  /**
   * join edge tuples with the graph sets of their sources
   */
  private static class JoinEdgeTupleWithSourceGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Long, Long>, Vertex<Long, VD>, Tuple3<Long,
      List<Long>, Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, List<Long>, Long> join(Tuple3<Long, Long, Long> tuple3,
      Vertex<Long, VD> vertex) throws Exception {
      return new Tuple3<>(tuple3.f0,
        (List<Long>) Lists.newArrayList(vertex.getValue().getGraphs()),
        tuple3.f2);
    }
  }

  /**
   * join edge tuples with the graph sets of their targets
   */
  private static class JoinEdgeTupleWithTargetGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, List<Long>, Long>, Vertex<Long, VD>,
      Tuple3<Long, List<Long>, List<Long>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, List<Long>, List<Long>> join(
      Tuple3<Long, List<Long>, Long> tuple3, Vertex<Long, VD> vertex) throws
      Exception {
      return new Tuple3<>(tuple3.f0, tuple3.f1,
        (List<Long>) Lists.newArrayList(vertex.getValue().getGraphs()));
    }
  }

  /**
   * map a subgraph to a set of longs, containing the identifier of
   * the subgraph
   */
  private static class MapSubgraphIdToSet<GD extends GraphData> implements
    MapFunction<Subgraph<Long, GD>, List<Long>> {
    @Override
    public List<Long> map(Subgraph<Long, GD> subgraph) throws Exception {
      List<Long> id = new ArrayList<>();
      id.add(subgraph.getId());
      return id;
    }
  }

  /**
   * reduce a dataset of sets of longs into a single set of longs
   */
  private static class ReduceSets implements ReduceFunction<List<Long>> {
    /**
     * {@inheritDoc}
     */
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
  private static class CrossEdgesWithGraphSet implements
    CrossFunction<Tuple3<Long, List<Long>, List<Long>>, List<Long>,
      Tuple4<Long, List<Long>, List<Long>, List<Long>>> {
    /**
     * {@inheritDoc}
     */
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
  private static class CheckEdgesSourceTargetGraphs implements
    FlatMapFunction<Tuple4<Long, List<Long>, List<Long>, List<Long>>,
      Tuple2<Long, List<Long>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(Tuple4<Long, List<Long>, List<Long>, List<Long>> tuple4,
      Collector<Tuple2<Long, List<Long>>> collector) throws Exception {
      List<Long> sourceGraphs = tuple4.f1;
      List<Long> targetGraphs = tuple4.f2;
      List<Long> newSubgraphs = tuple4.f3;
      boolean newGraphAdded = false;
      List<Long> toBeAddedGraphs = new ArrayList<>();
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
  private static class JoinEdgeTuplesWithEdges<ED extends EdgeData> implements
    JoinFunction<Edge<Long, ED>, Tuple2<Long, List<Long>>, Edge<Long, ED>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Edge<Long, ED> join(Edge<Long, ED> edge,
      Tuple2<Long, List<Long>> tuple2) throws Exception {
      return edge;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "OverlapSplitBy";
  }
}
