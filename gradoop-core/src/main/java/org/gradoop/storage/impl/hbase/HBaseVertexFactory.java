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

package org.gradoop.storage.impl.hbase;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.storage.api.PersistentVertexFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent vertex data representation.
 */
public class HBaseVertexFactory
  implements PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseVertex createVertex(VertexPojo inputVertex,
    Set<EdgePojo> outgoingEdges, Set<EdgePojo> incomingEdges) {
    checkNotNull(inputVertex, "Input vertex was null");
    checkNotNull(outgoingEdges, "Outgoing edges were null");
    checkNotNull(incomingEdges, "Incoming edges were null");
    return new HBaseVertex(inputVertex, outgoingEdges, incomingEdges);
  }
}
