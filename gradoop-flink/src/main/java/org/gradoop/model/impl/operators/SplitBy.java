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
 * operator used to split an EPGraph into a EPGraphCollection of graphs with
 * distinct vertices, uses a LongFromVertexFunction (result of this function
 * needs to be below 0)
 * to define the groups of
 * vertices that form the new graphs
 *
 * @param <VD> VertexData contains information about the vertex
 * @param <ED> EdgeData contains information about all edges of the vertex
 * @param <GD> GraphData contains information about all graphs of the vertex
 */
public class SplitBy<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * Flink execution enviroment
   */
  private ExecutionEnvironment env;
  /**
   * function, that defines a distinct mapping vertex -> long on the graph
   */
  private final UnaryFunction<Vertex<Long, VD>, Long> function;

  /**
   * public constructor
   *
   * @param env      ExecutionEnvironment
   * @param function LongFromVertexFunction
   */
  public SplitBy(UnaryFunction<Vertex<Long, VD>, Long> function,
    final ExecutionEnvironment env) {
    this.env = env;
    this.function = function;
  }

  /**
   * {@inheritDoc}
   * executes the SplitBy operation on a graph
   *
   * @param logicalGraph EPGraph
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

  private DataSet<Vertex<Long, VD>> computeNewVertices(
    LogicalGraph<VD, ED, GD> logicalGraph) {
    //get the Gelly graph and vertices
    final Graph<Long, VD, ED> graph = logicalGraph.getGellyGraph();
    DataSet<Vertex<Long, VD>> vertices = graph.getVertices();
    //add the new graphs to the vertices graph lists
    return vertices.map(new AddNewGraphsToVertexMapper<>(function));
  }

  private DataSet<Subgraph<Long, GD>> computeNewSubgraphs(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices) {
    // construct a KeySelector using the LongFromVertexFunction
    KeySelector<Vertex<Long, VD>, Long> propertySelector =
      new LongFromVertexSelector<>(function);
    //construct the list of subgraphs
    GraphDataFactory<GD> gdFactory = logicalGraph.getGraphDataFactory();
    return vertices.groupBy(propertySelector)
      .reduceGroup(new SubgraphsFromGroupsReducer<>(function, gdFactory));
  }

  private DataSet<Edge<Long, ED>> computeNewEdges(
    LogicalGraph<VD, ED, GD> logicalGraph, DataSet<Vertex<Long, VD>> vertices,
    DataSet<Subgraph<Long, GD>> subgraphs) {
    final Graph<Long, VD, ED> graph = logicalGraph.getGellyGraph();
    //construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      graph.getEdges().map(new EdgeToTupleMapper<ED>());
    //replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Long>> edgeGraphsVertex =
      edgeVertexVertex.join(vertices).where(1).equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    //replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, Set<Long>, Set<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex.join(vertices).where(2).equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    //transform the new subgraphs into a single set of long, containing all
    // the identifiers
    DataSet<Set<Long>> newSubgraphIdentifiers =
      subgraphs.map(new MapSubgraphIdToSet<GD>()).reduce(new ReduceSets());
    //construct new tuples containing the edge, the graphs of its source and
    //target vertex and the list of new graphs
    DataSet<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>> edgesWithSubgraphs =
      edgeGraphsGraphs.crossWithTiny(newSubgraphIdentifiers)
        .with(new CrossEdgesWithGraphSet());
    //remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<Long, Set<Long>>> newSubgraphs =
      edgesWithSubgraphs.flatMap(new CheckEdgesSourceTargetGraphs());
    //join the graph set tuples with the edges, add all new graphs to the
    //edge graph sets
    return graph.getEdges().join(newSubgraphs)
      .where(new KeySelectors.EdgeKeySelector<ED>()).equalTo(0)
      .with(new JoinEdgeTuplesWithEdges<ED>());
  }

  @Override
  public String getName() {
    return "SplitBy";
  }

  /**
   * applies the LongFromVertexFunction on a vertex
   *
   * @param <VD> vertex data
   */
  private static class LongFromVertexSelector<VD extends VertexData> implements
    KeySelector<Vertex<Long, VD>, Long> {
    /**
     * Unary Function
     */
    private UnaryFunction<Vertex<Long, VD>, Long> function;

    /**
     * Constructor
     *
     * @param function UnaryFunction maps a Vertex<Long, VD> --> Long
     */
    public LongFromVertexSelector(
      UnaryFunction<Vertex<Long, VD>, Long> function) {
      this.function = function;
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    public Long getKey(Vertex<Long, VD> vertex) throws Exception {
      return function.execute(vertex);
    }
  }

  /**
   * adds the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set
   *
   * @param <VD> vertex data
   */
  private static class AddNewGraphsToVertexMapper<VD extends VertexData>
    implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {
    /**
     * UnaryFunction
     */
    private UnaryFunction<Vertex<Long, VD>, Long> function;

    /**
     * Constructor
     *
     * @param function UnaryFunction maps a Vertex<Long, VD> -> Long
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<Vertex<Long, VD>, Long> function) {
      this.function = function;
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> vertex) throws Exception {
      Long labelPropIndex = function.execute(vertex);
      vertex.getValue().getGraphs().add(labelPropIndex);
      return vertex;
    }
  }

  /**
   * builds new graphs from vertices and the LongFromVertexFunction
   *
   * @param <VD> vertex data
   * @param <GD> graph data
   */
  private static class SubgraphsFromGroupsReducer<VD extends VertexData, GD
    extends GraphData> implements
    GroupReduceFunction<Vertex<Long, VD>, Subgraph<Long, GD>>,
    ResultTypeQueryable<Vertex<Long, VD>> {
    /**
     * UnaryFunction maps a Vertex<Long, VD> -> Long
     */
    private UnaryFunction<Vertex<Long, VD>, Long> function;
    /**
     * GraphDataFactory to build new GraphData
     */
    private final GraphDataFactory<GD> graphDataFactory;

    /**
     * Constructor
     *
     * @param function         UnaryFunction maps a Vertex<Long, VD> -> Long
     * @param graphDataFactory GraphDataFactory to build new GraphData
     */
    public SubgraphsFromGroupsReducer(
      UnaryFunction<Vertex<Long, VD>, Long> function,
      GraphDataFactory<GD> graphDataFactory) {
      this.function = function;
      this.graphDataFactory = graphDataFactory;
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
      Subgraph<Long, GD> newSubgraph = new Subgraph<>(labelPropIndex,
        graphDataFactory
          .createGraphData(labelPropIndex, "split graph " + labelPropIndex));
      collector.collect(newSubgraph);
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
   * maps the edges to tuples containing the edge id and the ids of its
   * source and target vertices
   *
   * @param <ED> edge data
   */
  private static class EdgeToTupleMapper<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Tuple3<Long, Long, Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Long, Long> map(Edge<Long, ED> edge) throws Exception {
      return new Tuple3<>(edge.getValue().getId(),
        edge.getValue().getSourceVertexId(),
        edge.getValue().getTargetVertexId());
    }
  }

  /**
   * replace the id of the source vertex of each edge with the set of graphs
   * this vertex belongs to
   *
   * @param <VD> vertex data
   */
  private static class JoinEdgeTupleWithSourceGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Long, Long>, Vertex<Long, VD>, Tuple3<Long,
      Set<Long>, Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Set<Long>, Long> join(Tuple3<Long, Long, Long> tuple3,
      Vertex<Long, VD> vertex) throws Exception {
      return new Tuple3<>(tuple3.f0, vertex.getValue().getGraphs(), tuple3.f2);
    }
  }

  /**
   * replace the id of the target vertex of each edge with the set of graphs
   * this vertex belongs to
   *
   * @param <VD> vertex data
   */
  private static class JoinEdgeTupleWithTargetGraphs<VD extends VertexData>
    implements
    JoinFunction<Tuple3<Long, Set<Long>, Long>, Vertex<Long, VD>,
      Tuple3<Long, Set<Long>, Set<Long>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, Set<Long>, Set<Long>> join(
      Tuple3<Long, Set<Long>, Long> tuple3, Vertex<Long, VD> vertex) throws
      Exception {
      return new Tuple3<>(tuple3.f0, tuple3.f1, vertex.getValue().getGraphs());
    }
  }

  /**
   * extract the id of each subgraph and put it into a Set<Long>
   *
   * @param <GD> graph data
   */
  private static class MapSubgraphIdToSet<GD extends GraphData> implements
    MapFunction<Subgraph<Long, GD>, Set<Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Long> map(Subgraph<Long, GD> subgraph) throws Exception {
      Set<Long> id = Sets.newHashSet();
      id.add(subgraph.getId());
      return id;
    }
  }

  /**
   * merge two sets into one by adding the contents of both into a new set
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
   * cross the edge tuples with a set containing the ids of the new graphs
   */
  private static class CrossEdgesWithGraphSet implements
    CrossFunction<Tuple3<Long, Set<Long>, Set<Long>>, Set<Long>, Tuple4<Long,
      Set<Long>, Set<Long>, Set<Long>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple4<Long, Set<Long>, Set<Long>, Set<Long>> cross(
      Tuple3<Long, Set<Long>, Set<Long>> tuple3, Set<Long> subgraphs) throws
      Exception {
      return new Tuple4<>(tuple3.f0, tuple3.f1, tuple3.f2, subgraphs);
    }
  }

  /**
   * check for each edge if its source and target vertices are in the same
   * new graph
   */
  private static class CheckEdgesSourceTargetGraphs implements
    FlatMapFunction<Tuple4<Long, Set<Long>, Set<Long>, Set<Long>>,
      Tuple2<Long, Set<Long>>> {
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
      if (newGraphAdded) {
        collector.collect(new Tuple2<>(tuple4.f0, toBeAddedGraphs));
      }
    }
  }

  /**
   * join the edge tuples with the edges, add the new graphs to the graph set
   *
   * @param <ED> edge data
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
