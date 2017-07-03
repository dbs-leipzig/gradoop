
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the overlap graph from two logical graphs.
 */
public class Overlap extends OverlapBase implements BinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of two input graphs. Vertex and edge equality is based on their
   * respective identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return graph with overlapping elements from both input graphs
   */
  @Override
  public LogicalGraph execute(
    LogicalGraph firstGraph, LogicalGraph secondGraph) {

    DataSet<GradoopId> graphIds = firstGraph.getGraphHead()
      .map(new Id<GraphHead>())
      .union(secondGraph.getGraphHead().map(new Id<GraphHead>()));

    return LogicalGraph.fromDataSets(
      getVertices(firstGraph.getVertices(), graphIds),
      getEdges(firstGraph.getEdges(), graphIds),
      firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Overlap.class.getName();
  }


}
