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
package org.gradoop.temporal.model.impl.functions.tpgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.Objects;

/**
 * Initializes a {@link EPGMEdge} from a {@link TemporalEdge} instance by discarding the temporal information.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;label;properties;graphIds")
public class TemporalEdgeToEdge implements MapFunction<TemporalEdge, EPGMEdge> {

  /**
   * Used to reduce instantiations.
   */
  private EPGMEdge reuse;

  /**
   * Creates an instance of this map function.
   *
   * @param edgeFactory A factory used to create EPGM edges.
   */
  public TemporalEdgeToEdge(EdgeFactory<EPGMEdge> edgeFactory) {
    this.reuse = Objects.requireNonNull(edgeFactory).createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
  }

  @Override
  public EPGMEdge map(TemporalEdge value) {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setSourceId(value.getSourceId());
    reuse.setTargetId(value.getTargetId());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    return reuse;
  }
}
