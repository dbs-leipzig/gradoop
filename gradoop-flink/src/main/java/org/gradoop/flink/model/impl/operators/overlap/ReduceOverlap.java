
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the overlap graph from a collection of logical graphs.
 */
public class ReduceOverlap extends OverlapBase implements
  ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing the overlapping vertex and edge sets
   * of the graphs contained in the given collection. Vertex and edge equality
   * is based on their respective identifiers.
   *
   * @param collection input collection
   * @return graph with overlapping elements from the input collection
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads.map(new Id<GraphHead>());

    return LogicalGraph.fromDataSets(
      getVertices(collection.getVertices(), graphIDs),
      getEdges(collection.getEdges(), graphIDs),
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ReduceOverlap.class.getName();
  }
}
