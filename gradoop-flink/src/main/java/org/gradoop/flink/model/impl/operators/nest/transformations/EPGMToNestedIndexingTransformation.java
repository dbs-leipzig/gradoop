package org.gradoop.flink.model.impl.operators.nest.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateElementToIdAndGraph;
import org.gradoop.flink.model.impl.operators.nest.functions.ExceptGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.SelfId;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.model.NormalizedGraph;

/**
 * Created by vasistas on 30/03/17.
 */
public class EPGMToNestedIndexingTransformation {

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public static NestedIndexing fromNormalizedGraph(NormalizedGraph logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHeads()
      .map(new Id<>());
    return initVertices(graphHeads,logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public static NestedIndexing fromLogicalGraph(LogicalGraph logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHead()
      .map(new Id<>());
    return initVertices(graphHeads,logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public static NestedIndexing fromGraphCollection(GraphCollection logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHeads()
      .map(new Id<>());
    return initVertices(graphHeads,logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Initializes the vertices and the edges with a similar procedure
   * @param h pre-evaluated heads
   * @param v Vertices
   * @param e Edges
   * @return the actual indices used
   */
  private static NestedIndexing initVertices(DataSet<GradoopId> h, DataSet<Vertex> v, DataSet<Edge>
    e) {
    //Creating the map for the graphheads appearing in the logical graph
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex = v
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new SelfId()).with(new LeftSide<>()).filter(new ExceptGraphHead());

    //Same as above
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge = e
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());

    return new NestedIndexing(h,graphHeadToVertex,graphHeadToEdge);
  }


}
