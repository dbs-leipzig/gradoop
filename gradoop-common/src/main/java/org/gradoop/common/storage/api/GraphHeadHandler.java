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

package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;

import java.io.IOException;
import java.util.Set;

/**
 * This class is responsible for reading and writing EPGM graph heads from
 * and to HBase.
 *
 * @param <G> EPGM graph head type
 */
public interface GraphHeadHandler<G extends EPGMGraphHead>
  extends ElementHandler {

  /**
   * Adds all vertex identifiers of the given graph to the given {@link Put}
   * and returns it.
   *
   * @param put       put to add vertex identifiers to
   * @param graphData graph data containing vertex identifiers
   * @return put with vertices
   */
  Put writeVertices(final Put put, final PersistentGraphHead graphData) throws
    IOException;

  /**
   * Reads the vertex identifiers from the given result.
   *
   * @param res HBase row
   * @return vertex identifiers stored in the given result
   */
  Set<Long> readVertices(final Result res);

  /**
   * Adds all edge identifiers of a given graph to the given {@link Put} and
   * returns it.
   *
   * @param put       put to add edge identifiers to
   * @param graphData graph data containing edge identifiers
   * @return put with edges
   */
  Put writeEdges(final Put put, final PersistentGraphHead graphData) throws
    IOException;

  /**
   * Reads the edge identifiers from the given result.
   *
   * @param res HBase row
   * @return edge identifiers stored in the given result
   */
  Set<Long> readEdges(final Result res);

  /**
   * Adds all graph information to the given {@link Put} and returns it.
   *
   * @param put       put to add graph data to
   * @param graphData graph data
   * @return put with graph data
   */
  Put writeGraphHead(final Put put, final PersistentGraphHead graphData) throws
    IOException;

  /**
   * Reads the graph data from the given result.
   *
   * @param res HBase row
   * @return graph entity
   */
  G readGraphHead(final Result res);

  /**
   * Returns the graph data factory used by this handler.
   *
   * @return graph data factory
   */
  EPGMGraphHeadFactory<G> getGraphHeadFactory();
}
