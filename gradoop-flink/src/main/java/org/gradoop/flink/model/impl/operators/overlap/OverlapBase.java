
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Base class for overlap operators that contains common logic.
 *
 * @see Overlap
 * @see ReduceOverlap
 */
public abstract class OverlapBase {

  /**
   * Filters vertices based on the given graph identifiers.
   *
   * @param vertices  vertices
   * @param ids       graph identifiers
   * @return filtered vertices
   */
  protected DataSet<Vertex> getVertices(DataSet<Vertex> vertices,
    DataSet<GradoopId> ids) {
    return vertices
      .filter(new InAllGraphsBroadcast<Vertex>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }

  /**
   * Filters edges based on the given graph identifiers.
   *
   * @param edges edges
   * @param ids   graph identifiers
   * @return filtered edges
   */
  protected DataSet<Edge> getEdges(DataSet<Edge> edges,
    DataSet<GradoopId> ids) {
    return edges
      .filter(new InAllGraphsBroadcast<Edge>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }
}
