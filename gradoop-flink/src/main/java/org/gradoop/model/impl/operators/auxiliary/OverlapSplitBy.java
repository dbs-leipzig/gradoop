package org.gradoop.model.impl.operators.auxiliary;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Split a LogicalGraph into an GraphCollection by a self defined mapping from
 * vertex to graph id.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class OverlapSplitBy<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  implements UnaryGraphToCollectionOperator<VD, ED, GD>, Serializable {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 513465233452L;
  /**
   * Self defined function for graph extraction
   */
  private final UnaryFunction<VD, List<Long>> function;

  /**
   * Constructor
   *
   * @param function self defined function
   */
  public OverlapSplitBy(UnaryFunction<VD, List<Long>> function) {
    this.function = function;
  }

  /**
   * execute the operator, split the EPGraph into an EPGraphCollection which
   * graphs can be overlapping
   *
   * @param graph the epGraph that will be split
   * @return a GraphCollection containing the newly created EPGraphs
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> graph) {
    // add all new subgraphs to the graph sets of the vertices
    DataSet<VD> vertices = computeNewVertices(graph);
    // construct the new subgraph objects
    DataSet<GD> subgraphs =
      computeNewSubgraphs(graph, vertices);
    // construct tuples containing the edge, source and target vertex ids
    DataSet<ED> edges =
      computeNewEdges(graph, vertices, subgraphs);
    return new GraphCollection<>(vertices, edges, subgraphs,
      graph.getConfig());
  }

  /**
   * compute the vertices in the new graphs created by the SplitBy and add
   * these graphs to the graph sets of the vertices
   *
   * @param graph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<VD> computeNewVertices(
    LogicalGraph<VD, ED, GD> graph) {
    // add the new graphs to the vertices graph lists
    return graph.getVertices()
      .map(new AddNewGraphsToVertexMapper<>(function));
  }

  /**
   * compute the new subgraphs created by the OverlapSplitBy
   *
   * @param graph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<GD> computeNewSubgraphs(
    LogicalGraph<VD, ED, GD> graph, DataSet<VD> vertices) {
    DataSet<Tuple1<Long>> newSubgraphIDs =
      vertices.flatMap(new VertexToGraphIDFlatMapper<>(function)).distinct();
    return newSubgraphIDs.map(new SubgraphMapper<>(graph.getConfig()
      .getGraphHeadFactory()));
  }

  /**
   * compute the new edges between all subgraphs
   *
   * @param graph the input graph
   * @param vertices     the computed vertices with their edges
   * @param graphHeads    the subgraphs of the vertices
   * @return all edges between all subgraphs
   */
  private DataSet<ED> computeNewEdges(
    LogicalGraph<VD, ED, GD> graph, DataSet<VD> vertices,
    DataSet<GD> graphHeads) {
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<Long, Long, Long>> edgeVertexVertex =
      graph.getEdges().map(new EdgeToTupleMapper<ED>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, List<Long>, Long>> edgeGraphsVertex = edgeVertexVertex
        .join(vertices)
        .where(1)
        .equalTo("id")
        .with(new JoinEdgeTupleWithSourceGraphs<VD>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<Long, List<Long>, List<Long>>> edgeGraphsGraphs =
      edgeGraphsVertex
        .join(vertices)
        .where(2)
        .equalTo("id")
        .with(new JoinEdgeTupleWithTargetGraphs<VD>());
    // transform the new grpah heads into a single set of long, containing all
    // the identifiers
    DataSet<List<Long>> newSubgraphIdentifiers = graphHeads
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
    return graph.getEdges()
      .join(newSubgraphs)
      .where(new EdgeKeySelector<ED>())
      .equalTo(0)
      .with(new JoinEdgeTuplesWithEdges<ED>());
  }

  /**
   * map the graph ids to subgraphs
   */
  private static class SubgraphMapper<GD extends EPGMGraphHead> implements
    MapFunction<Tuple1<Long>, GD>,
    ResultTypeQueryable<GD> {
    /**
     * EPGMGraphHeadFactory
     */
    private EPGMGraphHeadFactory<GD> graphHeadFactory;

    /**
     * Constructor
     *
     * @param graphHeadFactory actual EPGMGraphHeadFactory
     */
    public SubgraphMapper(EPGMGraphHeadFactory<GD> graphHeadFactory) {
      this.graphHeadFactory = graphHeadFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GD map(Tuple1<Long> idTuple) throws Exception {
      Long id = idTuple.f0;
      return graphHeadFactory.createGraphHead(id, "split graph " + id);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<GD> getProducedType() {
      return (TypeInformation<GD>) TypeExtractor.createTypeInfo(
        graphHeadFactory.getType());
    }
  }

  /**
   * maps the vertices to tuple containing all the graph ids
   */
  private static class VertexToGraphIDFlatMapper<VD extends EPGMVertex>
    implements
    FlatMapFunction<VD, Tuple1<Long>> {
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
    public void flatMap(VD vertex,
      Collector<Tuple1<Long>> collector) throws Exception {
      List<Long> graphIDSet = function.execute(vertex);
      for (Long id : graphIDSet) {
        collector.collect(new Tuple1<>(id));
      }
    }
  }

  /**
   * add the graph ids extracted by the LongFromVertexFunction to the
   * vertex graph set
   */
  private static class AddNewGraphsToVertexMapper<VD extends EPGMVertex>
    implements
    MapFunction<VD, VD> {
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
    public VD map(VD vertex) throws Exception {
      List<Long> labelPropIndex = function.execute(vertex);
      if (vertex.getGraphIds() == null) {
        vertex.setGraphs(new HashSet<Long>());
      }
      vertex.getGraphIds().addAll(labelPropIndex);
      return vertex;
    }
  }

  /**
   * transform an edge into a Tuple3 of edge id, source vertex and
   * target id
   */
  private static class EdgeToTupleMapper<ED extends EPGMEdge> implements
    MapFunction<ED, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(ED edge) throws Exception {
      return new Tuple3<>(edge.getId(),
        edge.getSourceVertexId(),
        edge.getTargetVertexId());
    }
  }

  /**
   * join edge tuples with the graph sets of their sources
   */
  private static class JoinEdgeTupleWithSourceGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<Long, Long, Long>, VD, Tuple3<Long,
      List<Long>, Long>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, List<Long>, Long> join(Tuple3<Long, Long, Long> tuple3,
      VD vertex) throws Exception {
      return new Tuple3<>(tuple3.f0,
        (List<Long>) Lists.newArrayList(vertex.getGraphIds()),
        tuple3.f2);
    }
  }

  /**
   * join edge tuples with the graph sets of their targets
   */
  private static class JoinEdgeTupleWithTargetGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<Long, List<Long>, Long>, VD,
      Tuple3<Long, List<Long>, List<Long>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<Long, List<Long>, List<Long>> join(
      Tuple3<Long, List<Long>, Long> tuple3, VD vertex) throws
      Exception {
      return new Tuple3<>(tuple3.f0, tuple3.f1,
        (List<Long>) Lists.newArrayList(vertex.getGraphIds()));
    }
  }

  /**
   * Map a graph head to a set of longs, containing the identifier of
   * the subgraph
   */
  private static class MapSubgraphIdToSet<GD extends EPGMGraphHead> implements
    MapFunction<GD, List<Long>> {
    @Override
    public List<Long> map(GD graphHead) throws Exception {
      List<Long> id = new ArrayList<>();
      id.add(graphHead.getId());
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
  @FunctionAnnotation.ForwardedFieldsFirst("*->*")
  private static class JoinEdgeTuplesWithEdges<ED extends EPGMEdge> implements
    JoinFunction<ED, Tuple2<Long, List<Long>>, ED> {
    /**
     * {@inheritDoc}
     */
    @Override
    public ED join(ED edge, Tuple2<Long, List<Long>> tuple2) throws Exception {
      return edge;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return OverlapSplitBy.class.getName();
  }
}
