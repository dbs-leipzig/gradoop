/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.split;

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
import org.gradoop.model.api.functions.UnaryFunction;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Split a LogicalGraph into an GraphCollection by a self defined mapping from
 * vertex to graph id.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class SplitWithOverlap
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E>, Serializable {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 513465233452L;
  /**
   * Self defined function for graph extraction
   */
  private final UnaryFunction<V, List<GradoopId>> function;

  /**
   * Constructor
   *
   * @param function self defined function
   */
  public SplitWithOverlap(UnaryFunction<V, List<GradoopId>> function) {
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
  public GraphCollection<G, V, E> execute(
    LogicalGraph<G, V, E> graph) {
    // add all new subgraphs to the graph sets of the vertices
    DataSet<V> vertices = computeNewVertices(graph);
    // construct the new subgraph objects
    DataSet<G> subgraphs =
      computeNewSubgraphs(graph, vertices);
    // construct tuples containing the edge, source and target vertex ids
    DataSet<E> edges =
      computeNewEdges(graph, vertices, subgraphs);

    return GraphCollection.fromDataSets(
      subgraphs, vertices, edges, graph.getConfig());
  }

  /**
   * compute the vertices in the new graphs created by the Split and add
   * these graphs to the graph sets of the vertices
   *
   * @param graph input graph
   * @return a DataSet containing all vertices, each vertex has one new graph
   * in its graph set
   */
  private DataSet<V> computeNewVertices(
    LogicalGraph<G, V, E> graph) {
    // add the new graphs to the vertices graph lists
    return graph.getVertices()
      .map(new AddNewGraphsToVertexMapper<>(function));
  }

  /**
   * compute the new subgraphs created by the SplitWithOverlap
   *
   * @param graph the input graph
   * @param vertices     the computed vertices with their graphs
   * @return a DataSet containing all newly created subgraphs
   */
  private DataSet<G> computeNewSubgraphs(
    LogicalGraph<G, V, E> graph, DataSet<V> vertices) {
    DataSet<Tuple1<GradoopId>> newSubgraphIDs =
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
  private DataSet<E> computeNewEdges(
    LogicalGraph<G, V, E> graph, DataSet<V> vertices,
    DataSet<G> graphHeads) {
    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> edgeVertexVertex =
      graph.getEdges().map(new EdgeToTupleMapper<E>());
    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<GradoopId, List<GradoopId>, GradoopId>> edgeGraphsVertex =
      edgeVertexVertex
        .join(vertices)
        .where(1)
        .equalTo("id")
        .with(new JoinEdgeTupleWithSourceGraphs<V>());
    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<GradoopId, List<GradoopId>, List<GradoopId>>>
      edgeGraphsGraphs = edgeGraphsVertex
        .join(vertices)
        .where(2)
        .equalTo("id")
        .with(new JoinEdgeTupleWithTargetGraphs<V>());
    // transform the new grpah heads into a single set of long, containing all
    // the identifiers
    DataSet<List<GradoopId>> newSubgraphIdentifiers = graphHeads
      .map(new MapSubgraphIdToSet<G>())
      .reduce(new ReduceSets());
    // construct new tuples containing the edge, the graphs of its source and
    // target vertex and the list of new graphs
    DataSet<Tuple4<GradoopId, List<GradoopId>, List<GradoopId>,
      List<GradoopId>>> edgesWithSubgraphs = edgeGraphsGraphs
      .crossWithTiny(newSubgraphIdentifiers).with(new CrossEdgesWithGraphSet());
    // remove all edges which source and target are not in at least one common
    // graph
    DataSet<Tuple2<GradoopId, List<GradoopId>>> newSubgraphs =
      edgesWithSubgraphs.flatMap(new CheckEdgesSourceTargetGraphs());
    // join the graph set tuples with the edges, add all new graphs to the
    // edge graph sets
    return graph.getEdges()
      .join(newSubgraphs)
      .where(new Id<E>())
      .equalTo(0)
      .with(new JoinEdgeTuplesWithEdges<E>());
  }

  /**
   * map the graph ids to subgraphs
   */
  private static class SubgraphMapper<GD extends EPGMGraphHead> implements
    MapFunction<Tuple1<GradoopId>, GD>,
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
    public GD map(Tuple1<GradoopId> idTuple) throws Exception {
      GradoopId id = idTuple.f0;
      return graphHeadFactory.initGraphHead(id, "split graph " + id);
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
    FlatMapFunction<VD, Tuple1<GradoopId>> {
    /**
     * Self defined Function
     */
    private UnaryFunction<VD, List<GradoopId>> function;

    /**
     * Constructor
     *
     * @param function actual defined Function
     */
    public VertexToGraphIDFlatMapper(
      UnaryFunction<VD, List<GradoopId>> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(VD vertex,
      Collector<Tuple1<GradoopId>> collector) throws Exception {
      List<GradoopId> graphIDSet = function.execute(vertex);
      for (GradoopId id : graphIDSet) {
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
    private UnaryFunction<VD, List<GradoopId>> function;

    /**
     * Constructor
     *
     * @param function actual defined Function
     */
    public AddNewGraphsToVertexMapper(
      UnaryFunction<VD, List<GradoopId>> function) {
      this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VD map(VD vertex) throws Exception {
      List<GradoopId> labelPropIndex = function.execute(vertex);
      if (vertex.getGraphIds() == null) {
        vertex.setGraphIds(new GradoopIdSet());
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
    MapFunction<ED, Tuple3<GradoopId, GradoopId, GradoopId>> {
    @Override
    public Tuple3<GradoopId, GradoopId, GradoopId>
    map(ED edge) throws Exception {

      return new Tuple3<>(edge.getId(),
        edge.getSourceId(),
        edge.getTargetId());
    }
  }

  /**
   * join edge tuples with the graph sets of their sources
   */
  private static class JoinEdgeTupleWithSourceGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<GradoopId, GradoopId, GradoopId>, VD, Tuple3<GradoopId,
      List<GradoopId>, GradoopId>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<GradoopId, List<GradoopId>, GradoopId> join(
      Tuple3<GradoopId, GradoopId, GradoopId> tuple3, VD vertex
    ) throws Exception {

      return new Tuple3<>(tuple3.f0,
        (List<GradoopId>) Lists.newArrayList(vertex.getGraphIds()),
        tuple3.f2);
    }
  }

  /**
   * join edge tuples with the graph sets of their targets
   */
  private static class JoinEdgeTupleWithTargetGraphs<VD extends EPGMVertex>
    implements
    JoinFunction<Tuple3<GradoopId, List<GradoopId>, GradoopId>, VD,
      Tuple3<GradoopId, List<GradoopId>, List<GradoopId>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<GradoopId, List<GradoopId>, List<GradoopId>> join(
      Tuple3<GradoopId, List<GradoopId>, GradoopId> tuple3, VD vertex) throws
      Exception {
      return new Tuple3<>(tuple3.f0, tuple3.f1,
        (List<GradoopId>) Lists.newArrayList(vertex.getGraphIds()));
    }
  }

  /**
   * Map a graph head to a set of longs, containing the identifier of
   * the subgraph
   */
  private static class MapSubgraphIdToSet<GD extends EPGMGraphHead> implements
    MapFunction<GD, List<GradoopId>> {
    @Override
    public List<GradoopId> map(GD graphHead) throws Exception {
      List<GradoopId> id = new ArrayList<>();
      id.add(graphHead.getId());
      return id;
    }
  }

  /**
   * reduce a dataset of sets of longs into a single set of longs
   */
  private static class ReduceSets implements ReduceFunction<List<GradoopId>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public List<GradoopId>
    reduce(List<GradoopId> set1, List<GradoopId> set2) throws Exception {

      set1.addAll(set2);
      return set1;
    }
  }

  /**
   * add the set of subgraphs to the edge tuples
   */
  private static class CrossEdgesWithGraphSet implements CrossFunction
    <Tuple3<GradoopId, List<GradoopId>, List<GradoopId>>, List<GradoopId>,
      Tuple4<GradoopId, List<GradoopId>, List<GradoopId>, List<GradoopId>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple4<GradoopId, List<GradoopId>, List<GradoopId>, List<GradoopId>>
    cross(Tuple3<GradoopId, List<GradoopId>, List<GradoopId>> tuple3,
      List<GradoopId> subgraphs) throws Exception {

      return new Tuple4<>(tuple3.f0, tuple3.f1, tuple3.f2, subgraphs);
    }
  }

  /**
   * check if the source and target vertices of the edges are in the
   * same new subgraphs and to update the edgesList
   */
  private static class CheckEdgesSourceTargetGraphs implements FlatMapFunction
    <Tuple4<GradoopId, List<GradoopId>, List<GradoopId>, List<GradoopId>>,
      Tuple2<GradoopId, List<GradoopId>>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(Tuple4<GradoopId, List<GradoopId>, List<GradoopId>,
      List<GradoopId>> tuple4, Collector<Tuple2<GradoopId,
      List<GradoopId>>> collector) throws Exception {

      List<GradoopId> sourceGraphs = tuple4.f1;
      List<GradoopId> targetGraphs = tuple4.f2;
      List<GradoopId> newSubgraphs = tuple4.f3;
      boolean newGraphAdded = false;
      List<GradoopId> toBeAddedGraphs = new ArrayList<>();
      for (GradoopId graph : newSubgraphs) {
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
    JoinFunction<ED, Tuple2<GradoopId, List<GradoopId>>, ED> {
    /**
     * {@inheritDoc}
     */
    @Override
    public ED
    join(ED edge, Tuple2<GradoopId, List<GradoopId>> tuple2) throws Exception {

      return edge;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SplitWithOverlap.class.getName();
  }
}
