package org.gradoop.model.impl.operators.collection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.keyselectors.GraphKeySelector;
import org.gradoop.model.impl.tuples.Subgraph;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
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
  protected DataSet<Subgraph<Long, GD>> computeNewSubgraphs() {
    return firstSubgraphs
      .union(secondSubgraphs)
      .groupBy(new GraphKeySelector<GD>())
      .reduceGroup(new SubgraphGroupReducer<GD>(2));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Intersect.class.getName();
  }
}
