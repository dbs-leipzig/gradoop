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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.collection.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.functions.keyselectors.GraphKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;

/**
 * Returns a collection with all logical graphs from two input collections.
 * Graph equality is based on their identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class Union<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends SetOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<VD> computeNewVertices(
    DataSet<GD> newGraphHeads) throws Exception {
    return firstCollection.getVertices()
      .union(secondCollection.getVertices())
      .distinct(new VertexKeySelector<VD>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<GD> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .distinct(new GraphKeySelector<GD>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<ED> computeNewEdges(DataSet<VD> newVertices) {
    return firstCollection.getEdges()
      .union(secondCollection.getEdges())
      .distinct(new EdgeKeySelector<ED>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Union.class.getName();
  }
}
