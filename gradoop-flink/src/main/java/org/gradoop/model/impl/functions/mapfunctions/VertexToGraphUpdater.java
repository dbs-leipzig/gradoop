/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.mapfunctions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.VertexData;

/**
 * Adds a given graph ID to the vertex and returns it.
 *
 * @param <VD> vertex data type
 */
public class VertexToGraphUpdater<VD extends VertexData> implements
  MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {

  /**
   * Graph identifier to add.
   */
  private final long newGraphID;

  /**
   * Creates map function
   *
   * @param newGraphID graph identifier to add to the vertex
   */
  public VertexToGraphUpdater(final long newGraphID) {
    this.newGraphID = newGraphID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex<Long, VD> map(Vertex<Long, VD> v) throws Exception {
    v.getValue().addGraph(newGraphID);
    return v;
  }
}
