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

package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent graph data representation.
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHeadFactory<G extends EPGMGraphHead>
  implements PersistentGraphHeadFactory<G> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseGraphHead<G> createGraphHead(G inputGraphHead,
    GradoopIdSet vertices, GradoopIdSet edges) {
    checkNotNull(inputGraphHead, "EPGMGraphHead was null");
    checkNotNull(vertices, "EPGMVertex identifiers were null");
    checkNotNull(edges, "EPGMEdge identifiers were null");
    return new HBaseGraphHead<>(inputGraphHead, vertices, edges);
  }
}
