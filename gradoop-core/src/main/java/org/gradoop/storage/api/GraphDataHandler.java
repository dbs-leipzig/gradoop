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

package org.gradoop.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;

import java.util.Set;

/**
 * VertexHandler is responsible for reading and writing EPG graphs from and to
 * HBase.
 *
 * @param <GD> graph data type
 */
public interface GraphDataHandler<GD extends GraphData> extends ElementHandler {
  /**
   * Adds all vertex identifiers of the given graph to the given {@link Put}
   * and returns it.
   *
   * @param put       put to add vertex identifiers to
   * @param graphData graph data containing vertex identifiers
   * @return put with vertices
   */
  Put writeVertices(final Put put, final PersistentGraphData graphData);

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
  Put writeEdges(final Put put, final PersistentGraphData graphData);

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
  Put writeGraphData(final Put put, final PersistentGraphData graphData);

  /**
   * Reads the graph data from the given result.
   *
   * @param res HBase row
   * @return graph entity
   */
  GD readGraphData(final Result res);

  /**
   * Returns the graph data factory used by this handler.
   *
   * @return graph data factory
   */
  GraphDataFactory<GD> getGraphDataFactory();
}
