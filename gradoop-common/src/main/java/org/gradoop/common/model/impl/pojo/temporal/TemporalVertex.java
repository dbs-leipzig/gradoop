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
 * TODO: descriptions
 */
public class TemporalVertex extends TemporalGraphElement implements EPGMVertex {

  public TemporalVertex() {
  }

  public TemporalVertex(GradoopId id, String label, Properties properties, GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
  }

  public TemporalVertex(GradoopId id, String label, Properties properties, GradoopIdSet graphIds,
    long validFrom) {
    super(id, label, properties, graphIds, validFrom);
  }

  public TemporalVertex(GradoopId id, String label, Properties properties, GradoopIdSet graphIds,
    long validFrom, long validTo) {
    super(id, label, properties, graphIds, validFrom, validTo);
  }

  public Vertex toVertex() {
    return new Vertex(getId(), getLabel(), getProperties(), getGraphIds());
  }

  public static TemporalVertex createVertex() {
    return new TemporalVertex(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  public static TemporalVertex fromNonTemporalVertex(Vertex vertex) {
    return new TemporalVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties(),
      vertex.getGraphIds());
  }
}
