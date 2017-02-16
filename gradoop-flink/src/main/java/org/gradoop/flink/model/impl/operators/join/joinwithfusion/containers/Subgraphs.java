package org.gradoop.flink.model.impl.operators.join.joinwithfusion.containers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.Self;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.CreateGraphHead;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions
  .DemultiplexEPGMElementBySubgraphId;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.ExtractIdFromTheRest;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.JoinGraphHeadToSubGraphVertices;



import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .Value0OfDisambiguationTuple;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Created by vasistas on 16/02/17.
 */
public class Subgraphs {

  private final DataSet<GradoopId> subgraphIds;
  private final DataSet<DisambiguationTupleWithVertexId> zoomOutViewAsVertex;
  private final DataSet<Tuple2<GradoopId, Edge>> zoomInViewAsEdges;
  private final DataSet<Tuple2<GradoopId,Vertex>> zoomInViewAsVertices;
  private final GradoopFlinkConfig config;
  private final DataSet<Vertex> allInternalVertices;

  /**
   *
   * @param patterns                  Collection of graphs approximating the
   *                                  subgraphs that we want to find in
   *                                  <code>graphContainingSubgraphs</code>
   * @param graphContainingSubgraphs  Graph where we want to create the
   *                                  subgraphs
   * @param logicalGraphId            The id of the graph containing the subgraphs
   */
  public Subgraphs(GraphCollection patterns, LogicalGraph graphContainingSubgraphs, GradoopId
    logicalGraphId) {
    this.config = patterns.getConfig();
    DataSet<GraphHead> graphHeadsFromPatterns = patterns.getGraphHeads();

    // ------------------------------------------------------
    // Return the vertices that are kept inside the subgraphs
    // ------------------------------------------------------
    this.allInternalVertices = patterns.getVertices()
      // From all the patterns, extract only those vertices that appear in the union
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(graphContainingSubgraphs.getGraphHead().map(new Id<>()), GraphContainmentFilterBroadcast.GRAPH_ID);

    // Associate to each element its graph…
    zoomInViewAsVertices = allInternalVertices
      .flatMap(new DemultiplexEPGMElementBySubgraphId<>())
      // and check if such subgraphs appear in the collection of patterns
      .join(graphHeadsFromPatterns)
      .where(new Value0Of2<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    //
    subgraphIds = zoomInViewAsVertices
        .partitionByHash(0)
        .mapPartition(new ExtractIdFromTheRest<>());

    // For each of the remaining graphid-s, I have to create the fused vertex
    // Creating the actual vertices (not expanded)
    zoomOutViewAsVertex = subgraphIds
        .join(graphHeadsFromPatterns)
        .where(new Self<>()).equalTo(new Id<>())
        .with(new JoinGraphHeadToSubGraphVertices(logicalGraphId));

    // associate to each edge its graphId <graphid,edge>
    zoomInViewAsEdges = patterns.getEdges()
      // From all the patterns, extract only those vertices that appear in the union
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(graphContainingSubgraphs.getGraphHead().map(new Id<>()),GraphContainmentFilterBroadcast.GRAPH_ID)
      // Associate to each element its graph…
      .flatMap(new DemultiplexEPGMElementBySubgraphId<>())
      // and check if such subgraphs appear in the collection of patterns
      // Then filter it again with the finalActualSubgraphs
      .join(subgraphIds)
      .where(new Value0Of2<>()).equalTo(new Self<>())
      .with(new LeftSide<>());
  }

  /**
   * View the subgraphs as vertices
   * @return
   */
  public DataSet<Vertex> getUpperLevelView() {
    return zoomOutViewAsVertex.map(new Value0OfDisambiguationTuple());
  }

  public DataSet<DisambiguationTupleWithVertexId> getDisambiguatedUpperLevelView() {
    return zoomOutViewAsVertex;
  }

  /**
   * The getter…
   * @return the id-s referring to all the graphs stored inside
   */
  public DataSet<GradoopId> getSubgraphIds() {
    return subgraphIds;
  }

  /**
   * The getter…
   * @return the summarized view for each subgraph as a vertex
   */
  public DataSet<DisambiguationTupleWithVertexId> getZoomOutViewAsVertex() {
    return zoomOutViewAsVertex;
  }

  /**
   * The getter…
   * @return  The association between the subgraph and the vertices that
   *          it contains
   */
  public DataSet<Tuple2<GradoopId, Edge>> getZoomInViewAsEdges() {
    return zoomInViewAsEdges;
  }

  /**
   * The getter…
   * @return  The association between the subgraph and the edges that
   *          it contains
   */
  public DataSet<Tuple2<GradoopId, Vertex>> getZoomInViewAsVertices() {
    return zoomInViewAsVertices;
  }

  /**
   * Given a vertex, it expands it into a logical graph
   * @param v   Vertex to be expanded
   * @return    Internal logical graph
   */
  public LogicalGraph expandVertex(Vertex v) {
    DataSet<GradoopId> gid = zoomInViewAsVertices
      .filter(x -> x.f1.equals(v))
      .map(new Value0Of2<>());
    DataSet<Vertex> vs = zoomInViewAsVertices
      .joinWithTiny(gid)
      .where(new Value0Of2<>()).equalTo(new Self<>())
      .with(new LeftSide<>())
      .map(new Value1Of2<>());
    DataSet<Edge> es = zoomInViewAsEdges
      .joinWithTiny(gid)
      .where(new Value0Of2<>()).equalTo(new Self<>())
      .with(new LeftSide<>())
      .map(new Value1Of2<>());
    DataSet<GraphHead> h = zoomOutViewAsVertex
      .joinWithTiny(gid)
      .where(new Value0OfDisambiguationTuple()).equalTo(new Self<>())
      .with(new LeftSide<>())
      .map(new CreateGraphHead2());
    return LogicalGraph.fromDataSets(h,vs,es,config);
  }

  /**
   * Given a graph's gradoopid, it expands it into a logical graph
   * @param id   Graph's id
   * @return     Corrispondant Logical Graph
   */
  public LogicalGraph subgraphAsLogicalGraph(GradoopId id) {
    DataSet<Vertex> vs = zoomInViewAsVertices
      .filter(x -> x.f0.equals(id))
      .map(new Value1Of2<>());
    DataSet<Edge> es = zoomInViewAsEdges
      .filter(x -> x.f0.equals(id))
      .map(new Value1Of2<>());
    DataSet<GraphHead> h = zoomOutViewAsVertex
      .filter(x -> x.f1.equals(id))
      .map(new Value0OfDisambiguationTuple())
      .map(new CreateGraphHead(id));
    return LogicalGraph.fromDataSets(h,vs,es,config);
  }

  public DataSet<Vertex> getAllVertices() {
    return allInternalVertices;
  }
}
