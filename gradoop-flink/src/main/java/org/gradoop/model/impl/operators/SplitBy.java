package org.gradoop.model.impl.operators;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.gradoop.GConstants;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.Subgraph;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Operator used to split a {@link LogicalGraph} into a {@link
 * GraphCollection} with non-overlapping vertex sets. The operator uses a
 * {@link LongFromVertexSelector} to retrieve the logical graph identifier
 * from the vertices in the input graph.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class SplitBy<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * Flink execution environment
   */
  private ExecutionEnvironment env;
  /**
   * Mapping from a vertex to a long value
   */
  private final UnaryFunction<Vertex<Long, VD>, Long> vertexToLongFunc;

  /**
   * Creates a split by instance.
   *
   * @param env              Flink Execution Environment
   * @param vertexToLongFunc Function to select a graph id from a vertex
   */
  public SplitBy(UnaryFunction<Vertex<Long, VD>, Long> vertexToLongFunc,
    final ExecutionEnvironment env) {
    this.env = env;
    this.vertexToLongFunc = vertexToLongFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    DataSet<Vertex<Long, VD>> vertices = computeNewVertices(logicalGraph);
    DataSet<Subgraph<Long, GD>> subgraphs =
      computeNewSubgraphs(logicalGraph, vertices);
    DataSet<Edge<Long, ED>> edges =
      computeNewEdges(logicalGraph, vertices, subgraphs);
    Graph<Long, VD, ED> newGraph = Graph.fromDataSet(vertices, edges, env);
    return new GraphCollection<>(newGraph, subgraphs,
      logicalGraph.getVertexDataFactory(), logicalGraph.getEdgeDataFactory(),
      logicalGraph.getGraphDataFactory(), env);
  }

  /**
   * Computes the vertices in the new graphs created by the SplitBy and add
   * these graphs to the graph sets of the vertices.
   *
   * @param logicalGraph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<Vertex<Long, VD>> computeNewVertices(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    // get the Gelly graph and vertices
    final Graph<Long, VD, ED> graph = logicalGraph.getGellyGraph();
    DataSet<Vertex<Long, VD>> vertices = graph.getVertices();
    // add the new graphs to the vertices graph lists
    return vertices.map(new AddNewGraphsToVertexMapper<>(vertexToLongFunc));
  }

  /**
   * Computes the new subgraphs created by the SplitBy.
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<Subgraph<Long, GD>> computeNewSubgraphs(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices) {
    // construct a KeySelector using the LongFromVertexFunction
    KeySelector<Vertex<Long, VD>, Long> propertySelector =
      new LongFromVertexSelector<>(vertexToLongFunc);
    // construct the list of subgraphs
    GraphDataFactory<GD> gdFactory = logicalGraph.getGraphDataFactory();
    return vertices.groupBy(propertySelector).reduceGroup(
      new SubgraphsFromGroupsReducer<>(vertexToLongFunc, gdFactory));
  }

  /**
   * Computes those edges where source and target are in the same newly created
   * graph.
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their graphs
   * @param subgraphs    the computed subgraphs
   * @return a DataSet containing all newly created edges, each edge has a
   * new graph in its graph set
   */
  private DataSet<Edge<Long, ED>> computeNewEdges(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices,
    DataSet<Subgraph<Long, GD>> subgraphs) {
    final Graph<Long, VD, ED> graph = logicalGraph.getGellyGraph();
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      graph.getEdges().map(new EdgeToTupleMapper<ED>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Long>> edgeGraphsVertex =
      edgeVertexVertex.join(vertices).where(1).equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Set<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex.join(vertices).where(2).equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    // transform the new subgraphs into a single set of long, containing all
    // the identifiers
    DataSet<Set<Long>> newSubgraphIdentifiers =
      subgraphs.map(new MapSubgraphIdToSet<GD>()).reduce(new ReduceSets());
    // construct new tuples containing the edge, the graphs of its source and
    // target vertex and the list of new graphs
    DataSet<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>> edgesWithSubgraphs =
      edgeGraphsGraphs.crossWithTiny(newSubgraphIdentifiers)
        .with(new CrossEdgesWithGraphSet());
    // remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<Long, Set<Long>>> newSubgraphs =
      edgesWithSubgraphs.flatMap(new CheckEdgesSourceTargetGraphs());
    // join the graph set tuples with the edges, add all new graphs to the
    // edge graph sets
    return graph.getEdges().join(newSubgraphs)
      .where(new KeySelectors.EdgeKeySelector<ED>()).equalTo(0)
      .with(new JoinEdgeTuplesWithEdges<ED>());
  }

  @Override
  public String getName() {
    return SplitBy.class.getName();
  }

  /**
   * Applies the user defined LongFromVertexFunction on a vertex
   *
   * @param <VD> vertex data
   */
  private static class LongFromVertexSelector<VD extends VertexData> implements
    KeySelector<Vertex<Long, VD>, Long> {
    /**
     * Unary Function
     */
    private final UnaryFunction<Vertex<Long, VD>, Long> function;

    /**
     * Creates a KeySelector instance.
     *
     * @param function Mapping from vertex to long value
     */
    public LongFromVertexSelector(
      UnaryFunction<Vertex<Long, VD>, Long> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getKey(Vertex<Long, VD> vertex) throws Exception {
      return function.execute(vertex);
    }
  }

  /**
   * Adds the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set.
   *
   * @param <VD> vertex data type
   */
  private static class AddNewGraphsToVertexMapper<VD extends VertexData>
    implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<Vertex<Long, VD>, Long> function;

    /**
     * Creates MapFunction instance.
     *
     * @param function Mapping from vertex to long value
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<Vertex<Long, VD>, Long> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> vertex) throws Exception {
      Long labelPropIndex = function.execute(vertex);
      if (vertex.getValue().getGraphs() == null) {
        vertex.getValue().setGraphs(Sets.newHashSet(labelPropIndex));
      } else {
        vertex.getValue().getGraphs().add(labelPropIndex);
      }
      return vertex;
    }
  }

  /**
   * Builds new graphs from vertices and the LongFromVertexFunction
   *
   * @param <VD> vertex data type
   * @param <GD> graph data type
   */
  private static class SubgraphsFromGroupsReducer<VD extends VertexData, GD
    extends GraphData> implements
    GroupReduceFunction<Vertex<Long, VD>, Subgraph<Long, GD>>,
    ResultTypeQueryable<Vertex<Long, VD>> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<Vertex<Long, VD>, Long> function;
    /**
     * GraphDataFactory to build new GraphData
     */
    private final GraphDataFactory<GD> graphDataFactory;
    /**
     * Reduce object instantiation.
     */
    private final Subgraph<Long, GD> reuseSubgraph;

    /**
     * Creates GroupReduceFunction instance.
     *
     * @param function         Mapping from vertex to long value
     * @param graphDataFactory GraphDataFactory to build new GraphData
     */
    public SubgraphsFromGroupsReducer(
      UnaryFunction<Vertex<Long, VD>, Long> function,
      GraphDataFactory<GD> graphDataFactory) {
      this.function = function;
      this.graphDataFactory = graphDataFactory;
      reuseSubgraph = new Subgraph<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<Vertex<Long, VD>> iterable,
      Collector<Subgraph<Long, GD>> collector) throws Exception {
      Iterator<Vertex<Long, VD>> it = iterable.iterator();
      Vertex<Long, VD> vertex = it.next();
      Long labelPropIndex = function.execute(vertex);
      reuseSubgraph.setId(labelPropIndex);
      reuseSubgraph.setValue(graphDataFactory.createGraphData(labelPropIndex,
        GConstants.DEFAULT_GRAPH_LABEL));
      collector.collect(reuseSubgraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Vertex<Long, VD>> getProducedType() {
      return new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(graphDataFactory.getType()));
    }
  }

  /**
   * Maps the edges to tuples containing the edge id and the ids of its
   * source and target vertices.
   *
   * @param <ED> edge data
   */
  @FunctionAnnotation.ForwardedFields("f0->f1;f1->f2")
  private static class EdgeToTupleMapper<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Tuple3<Long, Long, Long>> {
    /**
     * Reduce object instantiation.
     */
    private final Tuple3<Long, Long, Long> reuseTuple;

    /**
     * Creates MapFunction instance.
     */
    private EdgeToTupleMapper() {
      reuseTuple = new Tuple3<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Long, Long> map(Edge<Long, ED> edge) throws Exception {
      reuseTuple.f0 = edge.getValue().getId();
      reuseTuple.f1 = edge.getSource();
      reuseTuple.f2 = edge.getTarget();
      return reuseTuple;
    }
  }

  /**
   * Replace the id of the source vertex of each edge with the set of graphs
   * this vertex belongs to.
   *
   * @param <VD> vertex data type
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f2")
  private static class JoinEdgeTupleWithSourceGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Long, Long>, Vertex<Long, VD>, Tuple3<Long,
      Set<Long>, Long>> {
    /**
     * Reduce object instantiation
     */
    private final Tuple3<Long, Set<Long>, Long> reuseTuple;

    /**
     * Create JoinFunction instance.
     */
    private JoinEdgeTupleWithSourceGraphs() {
      reuseTuple = new Tuple3<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Set<Long>, Long> join(Tuple3<Long, Long, Long> tuple3,
      Vertex<Long, VD> vertex) throws Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = vertex.getValue().getGraphs();
      reuseTuple.f2 = tuple3.f2;
      return new Tuple3<>(tuple3.f0, vertex.getValue().getGraphs(), tuple3.f2);
    }
  }

  /**
   * Replace the id of the target vertex of each edge with the set of graphs
   * this vertex belongs to.
   *
   * @param <VD> vertex data type
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
  private static class JoinEdgeTupleWithTargetGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Set<Long>, Long>, Vertex<Long, VD>,
      Tuple3<Long, Set<Long>, Set<Long>>> {
    /**
     * Reduce object instantiations.
     */
    private final Tuple3<Long, Set<Long>, Set<Long>> reuseTuple;

    /**
     * Create JoinFunction instance.
     */
    private JoinEdgeTupleWithTargetGraphs() {
      reuseTuple = new Tuple3<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Set<Long>, Set<Long>> join(
      Tuple3<Long, Set<Long>, Long> tuple3, Vertex<Long, VD> vertex) throws
      Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = tuple3.f1;
      reuseTuple.f2 = vertex.getValue().getGraphs();
      return reuseTuple;
    }
  }

  /**
   * Extract the id of each subgraph and create a {@link Set} of them.
   *
   * @param <GD> graph data type
   */
  private static class MapSubgraphIdToSet<GD extends GraphData> implements
    MapFunction<Subgraph<Long, GD>, Set<Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Long> map(Subgraph<Long, GD> subgraph) throws Exception {
      Set<Long> idSet = Sets.newHashSet();
      idSet.add(subgraph.getId());
      return idSet;
    }
  }

  /**
   * Union two input sets.
   */
  private static class ReduceSets implements ReduceFunction<Set<Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Long> reduce(Set<Long> set1, Set<Long> set2) throws Exception {
      set1.addAll(set2);
      return set1;
    }
  }

  /**
   * Cross the edge tuples with a set containing the ids of the new graphs.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f2")
  private static class CrossEdgesWithGraphSet implements
    CrossFunction<Tuple3<Long, Set<Long>, Set<Long>>, Set<Long>, Tuple4<Long,
      Set<Long>, Set<Long>, Set<Long>>> {
    /**
     * Reduce object instantiations.
     */
    private final Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> reuseTuple;

    /**
     * Create CrossFunction instance.
     */
    private CrossEdgesWithGraphSet() {
      reuseTuple = new Tuple4<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> cross(
      Tuple3<Long, Set<Long>, Set<Long>> tuple3, Set<Long> subgraphs) throws
      Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = tuple3.f1;
      reuseTuple.f2 = tuple3.f2;
      reuseTuple.f3 = subgraphs;
      return reuseTuple;
    }
  }

  /**
   * Check for each edge if its source and target vertices are in the same
   * new graph.
   */
  @FunctionAnnotation.ForwardedFields("f0")
  private static class CheckEdgesSourceTargetGraphs implements
    FlatMapFunction<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>,
      Tuple2<Long, Set<Long>>> {
    /**
     * Reduce object instantiations.
     */
    private final Tuple2<Long, Set<Long>> reuseTuple;

    /**
     * Creates FlatMapFunction instance.
     */
    public CheckEdgesSourceTargetGraphs() {
      this.reuseTuple = new Tuple2<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> tuple4,
      Collector<Tuple2<Long, Set<Long>>> collector) throws Exception {
      Set<Long> sourceGraphs = tuple4.f1;
      Set<Long> targetGraphs = tuple4.f2;
      Set<Long> newSubgraphs = tuple4.f3;
      boolean newGraphAdded = false;
      Set<Long> toBeAddedGraphs = new HashSet<>();
      for (Long graph : newSubgraphs) {
        if (targetGraphs.contains(graph) && sourceGraphs.contains(graph)) {
          toBeAddedGraphs.add(graph);
          newGraphAdded = true;
        }
      }
      reuseTuple.f0 = tuple4.f0;
      reuseTuple.f1 = toBeAddedGraphs;
      if (newGraphAdded) {
        collector.collect(reuseTuple);
      }
    }
  }

  /**
   * Join the edge tuples with the edges, add the new graphs to the graph set.
   *
   * @param <ED> edge data type
   */
  private static class JoinEdgeTuplesWithEdges<ED extends EdgeData> implements
    JoinFunction<Edge<Long, ED>, Tuple2<Long, Set<Long>>, Edge<Long, ED>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Edge<Long, ED> join(Edge<Long, ED> edge,
      Tuple2<Long, Set<Long>> tuple2) throws Exception {
      edge.getValue().getGraphs().addAll(tuple2.f1);
      return edge;
    }
  }
}
