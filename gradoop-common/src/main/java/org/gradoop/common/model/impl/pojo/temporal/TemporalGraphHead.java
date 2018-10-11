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

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * TODO: descriptions
 */
public class TemporalGraphHead extends TemporalElement implements EPGMGraphHead {

  public TemporalGraphHead() {
  }

  public TemporalGraphHead(GradoopId id, String label, Properties properties) {
    super(id, label, properties);
  }

  public TemporalGraphHead(GradoopId id, String label, Properties properties, long validFrom) {
    super(id, label, properties, validFrom);
  }

  public TemporalGraphHead(GradoopId id, String label, Properties properties, long validFrom,
    long validTo) {
    super(id, label, properties, validFrom, validTo);
  }

  public GraphHead toGraphHead() {
    return new GraphHead(getId(), getLabel(), getProperties());
  }

  public static TemporalGraphHead createGraphHead() {
    return new TemporalGraphHead(GradoopId.get(), GradoopConstants.DEFAULT_GRAPH_LABEL, null);
  }

  public static TemporalGraphHead fromNonTemporalGraphHead(GraphHead graphHead) {
    return new TemporalGraphHead(
      graphHead.getId(),
      graphHead.getLabel(),
      graphHead.getProperties());
  }
}
