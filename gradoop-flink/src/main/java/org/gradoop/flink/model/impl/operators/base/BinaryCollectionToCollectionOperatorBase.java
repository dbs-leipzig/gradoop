/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
