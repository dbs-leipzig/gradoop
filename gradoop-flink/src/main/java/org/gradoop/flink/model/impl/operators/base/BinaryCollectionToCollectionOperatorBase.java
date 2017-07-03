
package org.gradoop.flink.model.impl.operators.base;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators
  .BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 */
public abstract class BinaryCollectionToCollectionOperatorBase
  implements BinaryCollectionToCollectionOperator {

  /**
   * First input collection.
   */
  protected GraphCollection firstCollection;
  /**
   * Second input collection.
   */
  protected GraphCollection secondCollection;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection execute(
    GraphCollection firstCollection,
    GraphCollection secondCollection) {

    // do some init stuff for the actual operator
    this.firstCollection = firstCollection;
    this.secondCollection = secondCollection;

    final DataSet<GraphHead> newGraphHeads = computeNewGraphHeads();
    final DataSet<Vertex> newVertices = computeNewVertices(newGraphHeads);
    final DataSet<Edge> newEdges = computeNewEdges(newVertices);

    return GraphCollection.fromDataSets(newGraphHeads, newVertices,
      newEdges, firstCollection.getConfig());
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newGraphHeads new graph heads
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads);

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<GraphHead> computeNewGraphHeads();

  /**
   * Overridden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<Edge> computeNewEdges(DataSet<Vertex> newVertices);

}
