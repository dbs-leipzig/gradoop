package org.gradoop.model.impl.operators.auxiliary;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.util.GConstants;

import java.util.Iterator;
import java.util.Set;

/**
 * Operator used to split a {@link LogicalGraph} into a {@link
 * GraphCollection} with non-overlapping vertex sets. The operator uses a
 * {@link GradoopIdFromVertexSelector} to retrieve the logical graph identifier
 * from the vertices in the input graph.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class SplitBy<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  implements UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * Flink execution environment
   */
  private ExecutionEnvironment env;
  /**
   * Mapping from a vertex to a long value
   */
  private final UnaryFunction<VD, GradoopId> vertexToLongFunc;

  /**
   * Creates a split by instance.
   *
   * @param env              Flink Execution Environment
   * @param vertexToLongFunc Function to select a graph id from a vertex
   */
  public SplitBy(UnaryFunction<VD, GradoopId> vertexToLongFunc,
    final ExecutionEnvironment env) {
    this.env = env;
    this.vertexToLongFunc = vertexToLongFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> graph) {
    DataSet<VD> vertices = computeNewVertices(graph);
    DataSet<GD> graphHeads = computeNewGraphHeads(graph, vertices);
    DataSet<ED> edges = computeNewEdges(graph, vertices, graphHeads);
    return GraphCollection.fromDataSets(vertices,
      edges, graphHeads, graph.getConfig());
  }

  /**
   * Computes the vertices in the new graphs created by the SplitBy and add
   * these graphs to the graph sets of the vertices.
   *
   * @param graph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<VD> computeNewVertices(
    LogicalGraph<VD, ED, GD> graph) {
    // add the new graphs to the vertices graph lists
    return graph.getVertices()
      .map(new AddNewGraphsToVertexMapper<>(vertexToLongFunc));
  }

  /**
   * Computes the new subgraphs created by the SplitBy.
   *
   * @param graph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<GD> computeNewGraphHeads(
    LogicalGraph<VD, ED, GD> graph, DataSet<VD> vertices) {
    // construct a KeySelector using the LongFromVertexFunction
    KeySelector<VD, GradoopId> propertySelector =
      new GradoopIdFromVertexSelector<>(vertexToLongFunc);
    // construct the list of subgraphs
    return vertices
      .groupBy(propertySelector)
      .reduceGroup(new SubgraphsFromGroupsReducer<>(
        vertexToLongFunc,
        graph.getConfig().getGraphHeadFactory()));
  }

  /**
   * Computes those edges where source and target are in the same newly created
   * graph.
   *
   * @param graph the input graph
   * @param vertices     the computed vertices with their graphs
   * @param graphHeads    the computed subgraphs
   * @return a DataSet containing all newly created edges, each edge has a
   * new graph in its graph set
   */
  private DataSet<ED> computeNewEdges(LogicalGraph<VD, ED, GD> graph,
    DataSet<VD> vertices,
    DataSet<GD> graphHeads) {
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> edgeVertexVertex =
      graph.getEdges().map(new EdgeToTupleMapper<ED>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<GradoopId, GradoopIds, GradoopId>> edgeGraphsVertex =
      edgeVertexVertex
        .join(vertices)
        .where(1)
        .equalTo(new VertexKeySelector<VD>())
        .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<GradoopId, GradoopIds, GradoopIds>> edgeGraphsGraphs =
      edgeGraphsVertex
        .join(vertices)
        .where(2)
        .equalTo(new VertexKeySelector<VD>())
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    // transform the new graph heads into a single set of long, containing all
    // the identifiers
    DataSet<GradoopIds> newSubgraphIdentifiers =
      graphHeads.map(new MapSubgraphIdToSet<GD>()).reduce(new ReduceSets());
    // construct new tuples containing the edge, the graphs of its source and
    // target vertex and the list of new graphs
    DataSet<Tuple4<GradoopId, GradoopIds, GradoopIds, GradoopIds>>
      edgesWithSubgraphs =
      edgeGraphsGraphs.crossWithTiny(newSubgraphIdentifiers)
        .with(new CrossEdgesWithGraphSet());
    // remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<GradoopId, GradoopIds>> newSubgraphs =
      edgesWithSubgraphs.flatMap(new CheckEdgesSourceTargetGraphs());
    // join the graph set tuples with the edges, add all new graphs to the
    // edge graph sets
    return graph.getEdges()
      .join(newSubgraphs)
      .where(new EdgeKeySelector<ED>())
      .equalTo(0)
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
  private static class GradoopIdFromVertexSelector<VD extends EPGMVertex>
    implements KeySelector<VD, GradoopId> {
    /**
     * Unary Function
     */
    private final UnaryFunction<VD, GradoopId> function;

    /**
     * Creates a KeySelector instance.
     *
     * @param function Mapping from vertex to long value
     */
    public GradoopIdFromVertexSelector(UnaryFunction<VD, GradoopId> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GradoopId getKey(VD vertex) throws Exception {
      return function.execute(vertex);
    }
  }

  /**
   * Adds the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set.
   *
   * @param <VD> vertex data type
   */
  private static class AddNewGraphsToVertexMapper<VD extends EPGMVertex>
    implements MapFunction<VD, VD> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<VD, GradoopId> function;

    /**
     * Creates MapFunction instance.
     *
     * @param function Mapping from vertex to long value
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<VD, GradoopId> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VD map(VD vertex) throws Exception {
      GradoopId labelPropIndex = function.execute(vertex);
      if (vertex.getGraphIds() == null) {
        vertex.setGraphIds(GradoopIds.fromExisting(labelPropIndex));
      } else {
        vertex.getGraphIds().add(labelPropIndex);
      }
      return vertex;
    }
  }

  /**
   * Builds new graphs from vertices and the LongFromVertexFunction
   *
   * @param <VD> EPGM vertex type
   * @param <GD> EPGM graph type
   */
  private static class SubgraphsFromGroupsReducer<VD extends EPGMVertex, GD
    extends EPGMGraphHead>
    implements GroupReduceFunction<VD, GD>,
    ResultTypeQueryable<GD> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<VD, GradoopId> function;
    /**
     * EPGMGraphHeadFactory to build new EPGMGraphHead
     */
    private final EPGMGraphHeadFactory<GD> graphHeadFactory;

    /**
     * Creates GroupReduceFunction instance.
     *
     * @param function         Mapping from vertex to long value
     * @param graphHeadFactory EPGMGraphHeadFactory to build new EPGMGraphHead
     */
    public SubgraphsFromGroupsReducer(
      UnaryFunction<VD, GradoopId> function,
      EPGMGraphHeadFactory<GD> graphHeadFactory) {
      this.function = function;
      this.graphHeadFactory = graphHeadFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VD> iterable,
      Collector<GD> collector) throws Exception {
      Iterator<VD> it = iterable.iterator();
      VD vertex = it.next();
      GradoopId labelPropIndex = function.execute(vertex);
      collector.collect(graphHeadFactory.createGraphHead(labelPropIndex,
        GConstants.DEFAULT_GRAPH_LABEL));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<GD> getProducedType() {
      return (TypeInformation<GD>) TypeExtractor.createTypeInfo(
        graphHeadFactory.getType());
    }
  }

  /**
   * Maps the edges to tuples containing the edge id and the ids of its
   * source and target vertices.
   *
   * @param <ED> edge data
   */
  @FunctionAnnotation.ForwardedFields("sourceVertexId->f1;targetVertexId->f2")
  private static class EdgeToTupleMapper<ED extends EPGMEdge> implements
    MapFunction<ED, Tuple3<GradoopId, GradoopId, GradoopId>> {
    /**
     * Reduce object instantiation.
     */
    private final Tuple3<GradoopId, GradoopId, GradoopId> reuseTuple;

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
    public Tuple3<GradoopId, GradoopId, GradoopId>
    map(ED edge) throws Exception {

      reuseTuple.f0 = edge.getId();
      reuseTuple.f1 = edge.getSourceVertexId();
      reuseTuple.f2 = edge.getTargetVertexId();
      return reuseTuple;
    }
  }

  /**
   * Replace the id of the source vertex of each edge with the set of graphs
   * this vertex belongs to.
   *
   * @param <VD> EPGM vertex type
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f2")
  private static class JoinEdgeTupleWithSourceGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<GradoopId, GradoopId, GradoopId>, VD, Tuple3<GradoopId,
      GradoopIds, GradoopId>> {
    /**
     * Reduce object instantiation
     */
    private final Tuple3<GradoopId, GradoopIds, GradoopId> reuseTuple;

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
    public Tuple3<GradoopId, GradoopIds, GradoopId> join(
      Tuple3<GradoopId, GradoopId, GradoopId> tuple3, VD vertex
    ) throws Exception {

      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = vertex.getGraphIds();
      reuseTuple.f2 = tuple3.f2;
      return new Tuple3<>(tuple3.f0, vertex.getGraphIds(), tuple3.f2);
    }
  }

  /**
   * Replace the id of the target vertex of each edge with the set of graphs
   * this vertex belongs to.
   *
   * @param <VD> vertex data type
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
  private static class JoinEdgeTupleWithTargetGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<GradoopId, GradoopIds, GradoopId>, VD,
      Tuple3<GradoopId, GradoopIds, GradoopIds>> {
    /**
     * Reduce object instantiations.
     */
    private final Tuple3<GradoopId, GradoopIds, GradoopIds> reuseTuple;

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
    public Tuple3<GradoopId, GradoopIds, GradoopIds> join(
      Tuple3<GradoopId, GradoopIds, GradoopId> tuple3, VD vertex) throws
      Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = tuple3.f1;
      reuseTuple.f2 = vertex.getGraphIds();
      return reuseTuple;
    }
  }

  /**
   * Extract the id of each subgraph and create a {@link Set} of them.
   *
   * @param <GD> graph data type
   */
  private static class MapSubgraphIdToSet<GD extends EPGMGraphHead> implements
    MapFunction<GD, GradoopIds> {
    /**
     * {@inheritDoc}
     */
    @Override
    public GradoopIds map(GD subgraph) throws Exception {
      GradoopIds idSet = new GradoopIds();
      idSet.add(subgraph.getId());
      return idSet;
    }
  }

  /**
   * Union two input sets.
   */
  private static class ReduceSets implements ReduceFunction<GradoopIds> {
    /**
     * {@inheritDoc}
     */
    @Override
    public GradoopIds
    reduce(GradoopIds set1, GradoopIds set2) throws Exception {

      set1.addAll(set2);
      return set1;
    }
  }

  /**
   * Cross the edge tuples with a set containing the ids of the new graphs.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f2")
  private static class CrossEdgesWithGraphSet implements CrossFunction
    <Tuple3<GradoopId, GradoopIds, GradoopIds>, GradoopIds, Tuple4<GradoopId,
      GradoopIds, GradoopIds, GradoopIds>> {
    /**
     * Reduce object instantiations.
     */
    private final
    Tuple4<GradoopId, GradoopIds, GradoopIds, GradoopIds> reuseTuple;

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
    public Tuple4<GradoopId, GradoopIds, GradoopIds, GradoopIds>
    cross(Tuple3<GradoopId, GradoopIds, GradoopIds> tuple3, GradoopIds subgraphs
    ) throws Exception {

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
    FlatMapFunction<Tuple4<GradoopId, GradoopIds, GradoopIds, GradoopIds>,
      Tuple2<GradoopId, GradoopIds>> {
    /**
     * Reduce object instantiations.
     */
    private final Tuple2<GradoopId, GradoopIds> reuseTuple;

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
    public void
    flatMap(Tuple4<GradoopId, GradoopIds, GradoopIds, GradoopIds> tuple4,
      Collector<Tuple2<GradoopId, GradoopIds>> collector) throws Exception {

      GradoopIds sourceGraphs = tuple4.f1;
      GradoopIds targetGraphs = tuple4.f2;
      GradoopIds newSubgraphs = tuple4.f3;
      boolean newGraphAdded = false;
      GradoopIds toBeAddedGraphs = new GradoopIds();
      for (GradoopId graph : newSubgraphs) {
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
  private static class JoinEdgeTuplesWithEdges<ED extends EPGMEdge> implements
    JoinFunction<ED, Tuple2<GradoopId, GradoopIds>, ED> {
    /**
     * {@inheritDoc}
     */
    @Override
    public ED join(ED edge,
      Tuple2<GradoopId, GradoopIds> tuple2) throws Exception {
      edge.getGraphIds().addAll(tuple2.f1);
      return edge;
    }
  }
}
