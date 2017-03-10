package org.gradoop.flink.model.impl.nested.datastructures;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.algorithms.BinaryOp;
import org.gradoop.flink.model.impl.nested.algorithms.UnaryOp;
import org.gradoop.flink.model.impl.nested.functions.SelfId;
import org.gradoop.flink.model.impl.nested.utils.LeftSideIfNotNull;
import org.gradoop.flink.model.impl.nested.utils.MapVertexAsGraphHead;

/**
 * Created by vasistas on 09/03/17.
 */
public class DataLake {

  /**
   * Logical Graph containing all the actual values. Represents the flattened network
   */
  private NormalizedGraph dataLake;

  /**
   * Given the datalake extracts only its ids for the computation
   */
  private IdGraphDatabase dataLakeIdDatabase;

  private DataLake(NormalizedGraph dataLake, IdGraphDatabase igdb) {
    this.dataLake = dataLake;
    this.dataLakeIdDatabase = igdb;
  }

  public DataLake(LogicalGraph dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = new IdGraphDatabase(this.dataLake);
  }

  public DataLake(GraphCollection dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = new IdGraphDatabase(this.dataLake);
  }

  public DataLake(NormalizedGraph dataLake) {
    this.dataLake = dataLake;
    dataLakeIdDatabase = new IdGraphDatabase(dataLake);
  }

  public DataSet<Edge> getEdges() {
    return dataLake.getEdges();
  }

  public DataSet<Vertex> getVerteices() {
    return dataLake.getVertices();
  }

  public void updateEdges(DataSet<Edge> edges, DataSet<Tuple2<GradoopId, GradoopId>> tuple2DataSet) {
    dataLake.updateEdgesWithUnion(edges);
    //dataLake = LogicalGraph.fromDataSets(dataLake.getHeads(),dataLake.getVertices(),dataLake
    //  .getEdges().union(edges),dataLake.getConf());
    dataLakeIdDatabase.addNewEdges(tuple2DataSet);
  }

  public IdGraphDatabase getIdDatabase() {
    return dataLakeIdDatabase;
  }

  public NormalizedGraph asNormalizedGraph() {
    return dataLake;
  }

  public DataLake extractGraphFromLabel(String label) {
    // the vertex could belong either to a former LogicalGraph (and hence, it appears as a vertex)
    // or as a vertex. The last case is always compliant, even for nested graphs.
    DataSet<GradoopId> extractHead = dataLake.getVertices()
      .filter(new ByLabel<>(label))
      .map(new Id<>());
    return extractGraphFromGradoopId(extractHead);
  }

  public DataLake extractGraphFromGradoopId(GradoopId gid) {
    DataSet<GradoopId> toPass = dataLake
      .getConfig()
      .getExecutionEnvironment()
      .fromElements(gid);
    return extractGraphFromGradoopId(toPass);
  }

  public DataLake extractGraphFromGradoopId(DataSet<GradoopId> graphHead) {

    // Extracting the informations for the IdDatabase
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = dataLakeIdDatabase
      .getGraphHeadToVertex()
      .joinWithTiny(graphHead)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>());
    DataSet<Tuple2<GradoopId, GradoopId>> edges = dataLakeIdDatabase
      .getGraphHeadToEdge()
      .joinWithTiny(graphHead)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>());

    /// Extracting the information
    DataSet<Vertex> subDataLakeVertices = dataLake.getVertices()
      .joinWithTiny(vertices)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>());

    DataSet<Edge> subDataLakeEdges = dataLake.getEdges()
      .joinWithTiny(edges)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>());

    // Checking if the id is a graph head.
    DataSet<GraphHead> actualGraphHead = dataLake.getGraphHeads()
      .joinWithTiny(graphHead)
      .where(new Id<>()).equalTo(new SelfId())
      .with(new LeftSide<>());

    DataSet<GraphHead> recreatedGraphHead = graphHead
      .joinWithHuge(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new RightSide<>())
      .map(new MapVertexAsGraphHead());

    DataSet<GraphHead> toReturnGraphHead =
      // The recreated vertex is always there. It is not said for the
      actualGraphHead.rightOuterJoin(recreatedGraphHead)
      .where((GraphHead x)->0).equalTo((GraphHead x)->0)
      .with(new LeftSideIfNotNull<GraphHead>());

    NormalizedGraph lg = new NormalizedGraph(toReturnGraphHead,
                                                subDataLakeVertices,
                                                subDataLakeEdges,
                                                dataLake.getConfig());

    IdGraphDatabase igdb = new IdGraphDatabase(graphHead,vertices,edges);

    return new DataLake(lg,igdb);
  }

  public <X extends BinaryOp> X run(X op) {
    return op.setDataLake(this);
  }

  public <X extends UnaryOp> X run(X op) {
    return op.setDataLake(this);
  }

}
