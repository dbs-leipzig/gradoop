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

package org.gradoop.model.impl.operators.union;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.operators.base.SetOperatorBase;

/**
 * Returns a collection with all logical graphs from two input collections.
 * Graph equality is based on their identifiers.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Union<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge>
  extends SetOperatorBase<G, V, E> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<V> computeNewVertices(DataSet<G> newGraphHeads) {
    return firstCollection.getVertices()
      .union(secondCollection.getVertices())
      .distinct(new Id<V>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<G> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .distinct(new Id<G>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<E> computeNewEdges(DataSet<V> newVertices) {
    return firstCollection.getEdges()
      .union(secondCollection.getEdges())
      .distinct(new Id<E>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Union.class.getName();
  }
}
