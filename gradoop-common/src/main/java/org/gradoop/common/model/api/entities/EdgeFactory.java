/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes {@link Edge} objects of a given type.
 *
 * @param <E> edge type
 */
public interface EdgeFactory<E extends Edge> extends ElementFactory<E> {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E createEdge(GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E initEdge(GradoopId id, GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties, GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties, GradoopIdSet graphIds);
}
