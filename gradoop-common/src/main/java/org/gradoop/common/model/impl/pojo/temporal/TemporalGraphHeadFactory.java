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

import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory for creating temporal graph head POJOs.
 */
public class TemporalGraphHeadFactory implements EPGMGraphHeadFactory<TemporalGraphHead>,
  Serializable {

  @Override
  public TemporalGraphHead createGraphHead() {
    return initGraphHead(GradoopId.get());
  }

  @Override
  public TemporalGraphHead initGraphHead(GradoopId id) {
    return initGraphHead(id, GradoopConstants.DEFAULT_GRAPH_LABEL, null);
  }

  @Override
  public TemporalGraphHead createGraphHead(String label) {
    return initGraphHead(GradoopId.get(), label);
  }

  @Override
  public TemporalGraphHead initGraphHead(GradoopId id, String label) {
    return initGraphHead(id, label, null);
  }

  @Override
  public TemporalGraphHead createGraphHead(String label, Properties properties) {
    return initGraphHead(GradoopId.get(), label, properties);
  }

  @Override
  public TemporalGraphHead initGraphHead(GradoopId id, String label, Properties properties) {
    return new TemporalGraphHead(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      properties,
      null,
      null
    );
  }

  @Override
  public Class<TemporalGraphHead> getType() {
    return TemporalGraphHead.class;
  }
}
