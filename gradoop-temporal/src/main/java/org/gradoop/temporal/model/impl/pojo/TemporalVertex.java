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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * POJO Implementation of a TPGM vertex.
 */
public class TemporalVertex extends TemporalGraphElement implements Vertex {

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
}
