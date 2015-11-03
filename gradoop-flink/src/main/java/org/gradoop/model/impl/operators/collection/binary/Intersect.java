package org.gradoop.model.impl.operators.collection.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.keyselectors.GraphKeySelector;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see IntersectUsingList
 */
public class Intersect<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
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
      .groupBy(new GraphKeySelector<GD>())
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
