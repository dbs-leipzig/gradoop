package org.gradoop.flink.model.impl.operators.join.joinwithfusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MapFunctionAddGraphElementToGraph2;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.CoGroupAssociateOldVerticesWithNewIds;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.CoGroupGraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MapVerticesAsTuplesWithNullId;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.FlatJoinSourceEdgeReference;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MapVertexToPairWithGraphId;

/**
 * Implements the disjunctive full join with
 *
 * Created by vasistas on 17/02/17.
 */
public class DisjucntiveFullJoinWithOtherFusion implements ReducibleBinaryGraphToGraphOperator {

  private final LogicalGraph left;
  private final LogicalGraph right;
  private final LogicalGraph gU;

  public DisjucntiveFullJoinWithOtherFusion(LogicalGraph left, LogicalGraph right) {
    this.left = left;
    this.right = right;
    gU = new Combination().execute(left,right);
  }

  @Override
  public String getName() {
    return DisjucntiveFullJoinWithOtherFusion.class.getName();
  }

  @Override
  public LogicalGraph execute(GraphCollection gc) {
    // Missing in the theoric definition: creating a new header
    GradoopId newGraphid = GradoopId.get();
    DataSet<GraphHead> gh = gU.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    // PHASE 1: Induced Subgraphs
    // Associate each vertex to its graph id
    DataSet<Tuple2<Vertex,GradoopId>> vWithGid = gc.getVertices()
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(gU.getGraphHead(), GraphContainmentFilterBroadcast.GRAPH_ID)
      .flatMap(new MapVertexToPairWithGraphId());

    // Associate each gid in gc.H to the merged vertices
    DataSet<Tuple2<Vertex,GradoopId>> nuWithGid  = vWithGid
      .coGroup(gc.getGraphHeads())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new CoGroupGraphHeadToVertex());

    // PHASE 2: Recreating the vertices
    DataSet<Vertex> vi = gU.getVertices()
      .filter(new NotInGraphBroadcast<>())
      .withBroadcastSet(gc.getGraphHeads(), GraphContainmentFilterBroadcast.GRAPH_ID);

    DataSet<Vertex> vToRet = nuWithGid
      .map(new Value0Of2<>())
      .union(vi)
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    DataSet<Tuple2<Vertex,GradoopId>> idJoin = vWithGid
      .coGroup(nuWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new CoGroupAssociateOldVerticesWithNewIds())
      .union(vi.map(new MapVerticesAsTuplesWithNullId()));

    // PHASE 3: Recreating the edges
    DataSet<Edge> edges = gU.getEdges()
      .filter(new NotInGraphBroadcast<>())
      .withBroadcastSet(gc.getGraphHeads(), GraphContainmentFilterBroadcast.GRAPH_ID)
      .fullOuterJoin(idJoin)
      .where(new SourceId<>()).equalTo(new Value0Of2<>())
      .with(new FlatJoinSourceEdgeReference(true))
      .fullOuterJoin(idJoin)
      .where(new TargetId<>()).equalTo(new Value0Of2<Vertex, GradoopId>())
      .with(new FlatJoinSourceEdgeReference(false))
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    return LogicalGraph.fromDataSets(gh,vToRet,edges,gU.getConfig());
  }


}
