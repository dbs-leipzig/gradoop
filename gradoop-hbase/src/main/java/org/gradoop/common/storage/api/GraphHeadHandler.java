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
