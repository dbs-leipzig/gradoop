package org.gradoop.model.impl.operators.collection.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.epgm.Id;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see IntersectBroadcast
 */
public class Intersect<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends SetOperator<VD, ED, GD> {

  /**
   * Computes new subgraphs by grouping both graph collections by graph
   * identifier and returning those graphs where the group contains more
   * than one element.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<GD> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .groupBy(new Id<GD>())
      .reduceGroup(new GraphHeadGroupReducer<GD>(2));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Intersect.class.getName();
  }
}
