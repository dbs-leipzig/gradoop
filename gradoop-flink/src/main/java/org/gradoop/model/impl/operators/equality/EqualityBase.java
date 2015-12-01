package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.counting.Tuple2WithObjectAnd1L;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Superclass of all equality operators.
 */
public abstract class EqualityBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * collection => (graphId, count)
   *
   * @param graphCollection input collection
   * @return graph id count
   */
  public
  AggregateOperator<Tuple2<GradoopId, Long>> getIdsWithCount(
    GraphCollection<G, V, E> graphCollection
  ) {

    return graphCollection
      .getGraphHeads()
      .map(new Id<G>())
      .map(new Tuple2WithObjectAnd1L<GradoopId>())
      .groupBy(0)
      .sum(1);
  }
}
