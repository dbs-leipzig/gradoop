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

package org.gradoop.flink.model.impl.operators.union;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.base.SetOperatorBase;

/**
 * Returns a collection with all logical graphs from two input collections.
 * Graph equality is based on their identifiers.
 */
public class Union extends SetOperatorBase {

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads) {
    return firstCollection.getVertices()
      .union(secondCollection.getVertices())
      .distinct(new Id<Vertex>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<GraphHead> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .distinct(new Id<GraphHead>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Edge> computeNewEdges(DataSet<Vertex> newVertices) {
    return firstCollection.getEdges()
      .union(secondCollection.getEdges())
      .distinct(new Id<Edge>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Union.class.getName();
  }
}
