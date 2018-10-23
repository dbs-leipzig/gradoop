/*
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
package org.gradoop.common.model.impl.pojo.temporal;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * POJO Implementation of a TPGM vertex.
 */
public class TemporalVertex extends TemporalGraphElement implements EPGMVertex {

  /**
   * Default constructor creates an empty {@link TemporalVertex} instance.
   */
  public TemporalVertex() {
    super();
  }

  /**
   * Creates a instance of a temporal vertex without temporal information.
   *
   * @param id the vertex identifier
   * @param label the vertex label
   * @param properties the vertex properties
   * @param graphIds identifiers of graphs this vertex is contained in
   * @param validFrom valid from unix timestamp in milliseconds
   * @param validTo valid to unix timestamp in milliseconds
   */
  public TemporalVertex(GradoopId id, String label, Properties properties, GradoopIdSet graphIds,
    Long validFrom, Long validTo) {
    super(id, label, properties, graphIds, validFrom, validTo);
  }

  /**
   * Static create method to avoid the usage of a factory class. Creates a temporal vertex instance
   * with default values.
   *
   * @return a temporal vertex instance with default values at its valid times
   */
  public static TemporalVertex createVertex() {
    return new TemporalVertex(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null,
      null, null, null);
  }

  /**
   * Helper function to create a TPGM vertex from an EPGM vertex.
   * The id, label and all other information will be inherited.
   *
   * @param vertex the EPGM vertex instance
   * @return a TPGM vertex instance with default values at its valid times
   */
  public static TemporalVertex fromNonTemporalVertex(Vertex vertex) {
    return new TemporalVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties(),
      vertex.getGraphIds(), null, null);
  }
}
