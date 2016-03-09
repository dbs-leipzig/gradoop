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

package org.gradoop.model.impl.operators.base;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public abstract class BinaryCollectionToCollectionOperatorBase<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  implements BinaryCollectionToCollectionOperator<G, V, E> {

  /**
   * First input collection.
   */
  protected GraphCollection<G, V, E> firstCollection;
  /**
   * Second input collection.
   */
  protected GraphCollection<G, V, E> secondCollection;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> execute(
    GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    // do some init stuff for the actual operator
    this.firstCollection = firstCollection;
    this.secondCollection = secondCollection;

    final DataSet<G> newGraphHeads = computeNewGraphHeads();
    final DataSet<V> newVertices = computeNewVertices(newGraphHeads);
    final DataSet<E> newEdges = computeNewEdges(newVertices);

    return GraphCollection.fromDataSets(newGraphHeads, newVertices,
      newEdges, firstCollection.getConfig());
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newGraphHeads new graph heads
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<V> computeNewVertices(DataSet<G> newGraphHeads);

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<G> computeNewGraphHeads();

  /**
   * Overridden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<E> computeNewEdges(DataSet<V> newVertices);

}
