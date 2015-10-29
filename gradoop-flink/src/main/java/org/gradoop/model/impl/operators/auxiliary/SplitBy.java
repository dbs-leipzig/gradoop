package org.gradoop.model.impl.operators.auxiliary;

import com.google.common.collect.Sets;
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
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.util.GConstants;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Operator used to split a {@link LogicalGraph} into a {@link
 * GraphCollection} with non-overlapping vertex sets. The operator uses a
 * {@link LongFromVertexSelector} to retrieve the logical graph identifier
 * from the vertices in the input graph.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class SplitBy<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * Flink execution environment
   */
  private ExecutionEnvironment env;
  /**
   * Mapping from a vertex to a long value
   */
  private final UnaryFunction<VD, Long> vertexToLongFunc;

  /**
   * Creates a split by instance.
   *
   * @param env              Flink Execution Environment
   * @param vertexToLongFunc Function to select a graph id from a vertex
   */
  public SplitBy(UnaryFunction<VD, Long> vertexToLongFunc,
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
    DataSet<VD> vertices = computeNewVertices(logicalGraph);
    DataSet<GD> graphHeads = computeNewGraphHeads(logicalGraph, vertices);
    DataSet<ED> edges = computeNewEdges(logicalGraph, vertices, graphHeads);
    return new GraphCollection<>(vertices, edges, graphHeads,
      logicalGraph.getConfig());
  }

  /**
   * Computes the vertices in the new graphs created by the SplitBy and add
   * these graphs to the graph sets of the vertices.
   *
   * @param logicalGraph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<VD> computeNewVertices(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    // add the new graphs to the vertices graph lists
    return logicalGraph.getVertices()
      .map(new AddNewGraphsToVertexMapper<>(vertexToLongFunc));
  }

  /**
   * Computes the new subgraphs created by the SplitBy.
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<GD> computeNewGraphHeads(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<VD> vertices) {
    // construct a KeySelector using the LongFromVertexFunction
    KeySelector<VD, Long> propertySelector =
      new LongFromVertexSelector<>(vertexToLongFunc);
    // construct the list of subgraphs
    return vertices
      .groupBy(propertySelector)
      .reduceGroup(new SubgraphsFromGroupsReducer<>(
        vertexToLongFunc,
        logicalGraph.getConfig().getGraphHeadFactory()));
  }

  /**
   * Computes those edges where source and target are in the same newly created
   * graph.
   *
   * @param logicalGraph the input graph
   * @param vertices     the computed vertices with their graphs
   * @param graphHeads    the computed subgraphs
   * @return a DataSet containing all newly created edges, each edge has a
   * new graph in its graph set
   */
  private DataSet<ED> computeNewEdges(LogicalGraph<VD, ED, GD> logicalGraph,
    DataSet<VD> vertices,
    DataSet<GD> graphHeads) {
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      logicalGraph.getEdges().map(new EdgeToTupleMapper<ED>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Long>> edgeGraphsVertex = edgeVertexVertex
      .join(vertices)
      .where(1)
      .equalTo(new VertexKeySelector<VD>())
      .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Set<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex
        .join(vertices)
        .where(2)
        .equalTo(new VertexKeySelector<VD>())
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    // transform the new graph heads into a single set of long, containing all
    // the identifiers
    DataSet<Set<Long>> newSubgraphIdentifiers =
      graphHeads.map(new MapSubgraphIdToSet<GD>()).reduce(new ReduceSets());
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
    return logicalGraph.getEdges()
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
  private static class LongFromVertexSelector<VD extends VertexData> implements
    KeySelector<VD, Long> {
    /**
     * Unary Function
     */
    private final UnaryFunction<VD, Long> function;

    /**
     * Creates a KeySelector instance.
     *
     * @param function Mapping from vertex to long value
     */
    public LongFromVertexSelector(
      UnaryFunction<VD, Long> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getKey(VD vertex) throws Exception {
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
    implements MapFunction<VD, VD> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<VD, Long> function;

    /**
     * Creates MapFunction instance.
     *
     * @param function Mapping from vertex to long value
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<VD, Long> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VD map(VD vertex) throws Exception {
      Long labelPropIndex = function.execute(vertex);
      if (vertex.getGraphs() == null) {
        vertex.setGraphs(Sets.newHashSet(labelPropIndex));
      } else {
        vertex.getGraphs().add(labelPropIndex);
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
  private static class SubgraphsFromGroupsReducer<VD extends VertexData, GD
    extends GraphData>
    implements GroupReduceFunction<VD, GD>,
    ResultTypeQueryable<GD> {
    /**
     * Mapping from vertex to long value
     */
    private final UnaryFunction<VD, Long> function;
    /**
     * GraphDataFactory to build new GraphData
     */
    private final GraphDataFactory<GD> graphDataFactory;

    /**
     * Creates GroupReduceFunction instance.
     *
     * @param function         Mapping from vertex to long value
     * @param graphDataFactory GraphDataFactory to build new GraphData
     */
    public SubgraphsFromGroupsReducer(
      UnaryFunction<VD, Long> function,
      GraphDataFactory<GD> graphDataFactory) {
      this.function = function;
      this.graphDataFactory = graphDataFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VD> iterable,
      Collector<GD> collector) throws Exception {
      Iterator<VD> it = iterable.iterator();
      VD vertex = it.next();
      Long labelPropIndex = function.execute(vertex);
      collector.collect(graphDataFactory.createGraphData(labelPropIndex,
        GConstants.DEFAULT_GRAPH_LABEL));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<GD> getProducedType() {
      return (TypeInformation<GD>) TypeExtractor.createTypeInfo(
        graphDataFactory.getType());
    }
  }

  /**
   * Maps the edges to tuples containing the edge id and the ids of its
   * source and target vertices.
   *
   * @param <ED> edge data
   */
  @FunctionAnnotation.ForwardedFields("sourceVertexId->f1;targetVertexId->f2")
  private static class EdgeToTupleMapper<ED extends EdgeData> implements
    MapFunction<ED, Tuple3<Long, Long, Long>> {
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
    public Tuple3<Long, Long, Long> map(ED edge) throws Exception {
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
  private static class JoinEdgeTupleWithSourceGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Long, Long>, VD, Tuple3<Long,
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
      VD vertex) throws Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = vertex.getGraphs();
      reuseTuple.f2 = tuple3.f2;
      return new Tuple3<>(tuple3.f0, vertex.getGraphs(), tuple3.f2);
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
    JoinFunction<Tuple3<Long, Set<Long>, Long>, VD,
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
      Tuple3<Long, Set<Long>, Long> tuple3, VD vertex) throws
      Exception {
      reuseTuple.f0 = tuple3.f0;
      reuseTuple.f1 = tuple3.f1;
      reuseTuple.f2 = vertex.getGraphs();
      return reuseTuple;
    }
  }

  /**
   * Extract the id of each subgraph and create a {@link Set} of them.
   *
   * @param <GD> graph data type
   */
  private static class MapSubgraphIdToSet<GD extends GraphData> implements
    MapFunction<GD, Set<Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Long> map(GD subgraph) throws Exception {
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
    JoinFunction<ED, Tuple2<Long, Set<Long>>, ED> {
    /**
     * {@inheritDoc}
     */
    @Override
    public ED join(ED edge,
      Tuple2<Long, Set<Long>> tuple2) throws Exception {
      edge.getGraphs().addAll(tuple2.f1);
      return edge;
    }
  }
}
