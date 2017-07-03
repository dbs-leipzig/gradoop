
package org.gradoop.flink.model.impl.operators.intersection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * This operator implementation requires that a list of subgraph identifiers
 * in the resulting graph collections fits into the workers main memory.
 */
public class IntersectionBroadcast extends Intersection {

  @Override
  protected DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newSubgraphs) {

    DataSet<GradoopId> ids = secondCollection.getGraphHeads()
      .map(new Id<GraphHead>());

    return firstCollection.getVertices()
      .filter(new InAnyGraphBroadcast<Vertex>())
      .withBroadcastSet(ids, GraphsContainmentFilterBroadcast.GRAPH_IDS);
  }

  @Override
  public String getName() {
    return IntersectionBroadcast.class.getName();
  }
}
