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

import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory for creating temporal vertex POJOs.
 */
public class TemporalVertexFactory implements EPGMVertexFactory<TemporalVertex>, Serializable {

  @Override
  public TemporalVertex createVertex() {
    return initVertex(GradoopId.get());
  }

  @Override
  public TemporalVertex initVertex(GradoopId id) {
    return initVertex(id, GradoopConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  @Override
  public TemporalVertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label) {
    return initVertex(id, label, null, null);
  }

  @Override
  public TemporalVertex createVertex(String label, Properties properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, Properties properties) {
    return initVertex(id, label, properties, null);
  }

  @Override
  public TemporalVertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, GradoopIdSet graphIds) {
    return initVertex(id, label, null, graphIds);
  }

  @Override
  public TemporalVertex createVertex(String label, Properties properties, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, Properties properties,
    GradoopIdSet graphIds) {
    return new TemporalVertex(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      properties,
      graphIds,
      null,
      null
    );
  }

  @Override
  public Class<TemporalVertex> getType() {
    return TemporalVertex.class;
  }
}
