/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.util.Iterator;

/**
 * definition of graph store output
 *
 * @param <OG> graph head(output)
 * @param <OV> graph vertex(output)
 * @param <OE> graph edge(output)
 */
public interface EPGMGraphOutput<
  OG extends EPGMGraphHead,
  OV extends EPGMVertex,
  OE extends EPGMEdge> {

  /**
   * Reads a graph data entity from the EPGM store using the given graph
   * identifier. If {@code graphId} does not exist, {@code null} is returned.
   *
   * @param graphId graph identifier
   * @return graph data entity or {@code null} if there is no entity with the
   * given {@code graphId}
   */
  OG readGraph(final GradoopId graphId);

  /**
   * Reads a vertex data entity from the EPGM store using the given vertex
   * identifier. If {@code vertexId} does not exist, {@code null} is returned.
   *
   * @param vertexId vertex identifier
   * @return vertex data entity or {@code null} if there is no entity with the
   * given {@code vertexId}
   */
  OV readVertex(final GradoopId vertexId);

  /**
   * Reads an edge data entity from the EPGM store using the given edge
   * identifier. If {@code edgeId} does not exist, {@code null} is returned.
   *
   * @param edgeId edge identifier
   * @return edge data entity or {@code null} if there is no entity with the
   * given {@code edgeId}
   */
  OE readEdge(final GradoopId edgeId);

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<OV> getVertexSpace() throws InterruptedException, IOException, ClassNotFoundException;

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<OV> getVertexSpace(int cacheSize) throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<OE> getEdgeSpace() throws InterruptedException, IOException, ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @param cacheSize cache size for HBase scan
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<OE> getEdgeSpace(int cacheSize) throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<OG> getGraphSpace() throws InterruptedException, IOException, ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<OG> getGraphSpace(int cacheSize) throws InterruptedException, IOException,
    ClassNotFoundException;

}
