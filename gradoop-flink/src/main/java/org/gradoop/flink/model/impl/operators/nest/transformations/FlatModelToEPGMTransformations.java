package org.gradoop.flink.model.impl.operators.nest.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.nest.functions.filter.ByLabels;
import org.gradoop.flink.model.impl.operators.nest.functions.LeftSideIfNotNull;
import org.gradoop.flink.model.impl.operators.nest.functions.map.MapVertexAsGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.keys.SelfId;
import org.gradoop.flink.model.impl.operators.nest.model.FlatModel;
import org.gradoop.flink.model.impl.operators.nest.model.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.model.NormalizedGraph;

/**
 * This class contains all the transformations required to convert a FlatModel into a EPGM
 * representation
 */
public class FlatModelToEPGMTransformations {

  /**
   * Extract a (sub) FlatModel from the main one having the designed label
   * @param dl      The abstraction over the EPGM model keeping track of the changes
   * @param label   Designed label
   * @return  (sub) FlatModel havint the parameter as a specific label
   */
  public static FlatModel extractGraphFromLabel(FlatModel dl, String label) {
    // the vertex could belong either to a former LogicalGraph (and hence, it appears as a vertex)
    // or as a vertex. The last case is always compliant, even for nested graphs.
    DataSet<GradoopId> extractHead = dl.asNormalizedGraph().getVertices()
      .filter(new ByLabel<>(label))
      .map(new Id<>());
    return extractGraphFromGradoopId(dl,extractHead);
  }

  /**
   * Extracts a (sub) FlatModel from the actual one,
   * @param dl          The abstraction over the EPGM model keeping track of the changes
   * @param labels      The graphs' unique labels that we want to extract for each subgraph
   * @return            A FlatModel
   */
  public static FlatModel extractGraphFromLabel(FlatModel dl, String... labels) {
    // the vertex could belong either to a former LogicalGraph (and hence, it appears as a vertex)
    // or as a vertex. The last case is always compliant, even for nested graphs.
    DataSet<GradoopId> extractHead = dl.asNormalizedGraph().getVertices()
      .filter(new ByLabels<>(labels))
      .map(new Id<>());
    return extractGraphFromGradoopId(dl,extractHead);
  }

  /**
   * Extracts a (sub) FlatModel from the actual one,
   * @param dl          The abstraction over the EPGM model keeping track of the changes
   * @param gid         The graphs' graph id that is interesting for our operation
   * @return            A FlatModel
   */
  public static FlatModel extractGraphFromGradoopId(FlatModel dl, GradoopId gid) {
    DataSet<GradoopId> toPass = dl.asNormalizedGraph()
      .getConfig()
      .getExecutionEnvironment()
      .fromElements(gid);
    return extractGraphFromGradoopId(dl,toPass);
  }

  /**
   * Extracts a (sub) FlatModel from the actual one,
   * @param dl          The abstraction over the EPGM model keeping track of the changes
   * @param graphHead   The graphs' graph ids that are interesting for our operation
   * @return            A FlatModel
   */
  public static FlatModel extractGraphFromGradoopId(FlatModel dl, DataSet<GradoopId> graphHead) {
    NestedIndexing dataLakeIdDatabase = dl.getIdDatabase();
    NormalizedGraph flatLands = dl.asNormalizedGraph();

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
    DataSet<Vertex> subDataLakeVertices = flatLands.getVertices()
      .joinWithTiny(vertices)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .distinct(new Id<>());

    DataSet<Edge> subDataLakeEdges = flatLands.getEdges()
      .joinWithTiny(edges)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .distinct(new Id<>());

    // Checking if the id is a graph head.
    DataSet<GraphHead> actualGraphHead = flatLands.getGraphHeads()
      .joinWithTiny(graphHead)
      .where(new Id<>()).equalTo(new SelfId())
      .with(new LeftSide<>());

    DataSet<GraphHead> recreatedGraphHead = graphHead
      .joinWithHuge(flatLands.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new RightSide<>())
      .map(new MapVertexAsGraphHead());

    DataSet<GraphHead> toReturnGraphHead =
      // The recreated vertex is always there. It is not said for the
      actualGraphHead.rightOuterJoin(recreatedGraphHead)
        .where((GraphHead x)->0).equalTo((GraphHead x)->0)
        .with(new LeftSideIfNotNull<>())
        .distinct(new Id<>());

    NormalizedGraph lg = new NormalizedGraph(toReturnGraphHead,
      subDataLakeVertices,
      subDataLakeEdges,
      flatLands.getConfig());

    NestedIndexing igdb = new NestedIndexing(graphHead, vertices, edges);

    return new FlatModel(lg, igdb);
  }

}
