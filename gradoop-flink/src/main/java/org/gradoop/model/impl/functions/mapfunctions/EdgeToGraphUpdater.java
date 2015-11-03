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
import org.gradoop.model.api.EPGMEdge;

/**
 * Adds a given graph ID to the edge and returns it.
 *
 * @param <ED> EPGM edge type
 */
public class EdgeToGraphUpdater<ED extends EPGMEdge> implements
  MapFunction<ED, ED> {

  /**
   * Graph identifier to add.
   */
  private final long newGraphID;

  /**
   * Creates map function
   *
   * @param newGraphID graph identifier to add
   */
  public EdgeToGraphUpdater(final long newGraphID) {
    this.newGraphID = newGraphID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ED map(ED e) throws Exception {
    e.addGraph(newGraphID);
    return e;
  }
}
