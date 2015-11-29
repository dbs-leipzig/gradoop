package org.gradoop.model.impl.operators.intersection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.operators.base.SetOperatorBase;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @see IntersectionBroadcast
 */
public class Intersection<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends SetOperatorBase<G, V, E> {

  /**
   * Computes new subgraphs by grouping both graph collections by graph
   * identifier and returning those graphs where the group contains more
   * than one element.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<G> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .groupBy(new Id<G>())
      .reduceGroup(new GraphHeadGroupReducer<G>(2));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Intersection.class.getName();
  }
}
