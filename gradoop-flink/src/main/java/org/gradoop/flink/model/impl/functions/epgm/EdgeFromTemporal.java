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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;

/**
 * Initializes a {@link Edge} from a {@link TemporalEdge} instance by discarding the temporal
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;label;properties;graphIds")
public class EdgeFromTemporal implements MapFunction<TemporalEdge, Edge> {

  /**
   * Used to reduce instantiations
   */
  private Edge reuse;

  /**
   * Creates an instance of the EdgeFromTemporal map function
   */
  public EdgeFromTemporal() {
    this.reuse = new Edge();
  }

  @Override
  public Edge map(TemporalEdge value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setSourceId(value.getSourceId());
    reuse.setTargetId(value.getTargetId());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    return reuse;
  }
}
