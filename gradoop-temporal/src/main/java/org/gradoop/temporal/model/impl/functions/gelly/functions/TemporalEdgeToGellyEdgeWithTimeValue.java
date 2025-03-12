/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.functions.gelly.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Maps Temporal edge to a Gelly edge consisting of Gradoop source and target
 * identifier and Valid Time as edge value.
 *
 * @param <E>  Gradoop edge type.
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1")
public class TemporalEdgeToGellyEdgeWithTimeValue<E extends TemporalEdge>
        implements TemporalEdgeToGellyEdge<E, Tuple2<Long, Long>> {

  /**
   * Reduce object instantiations.
   */
  private final org.apache.flink.graph.Edge<GradoopId, Tuple2<Long, Long>> reuseEdge;

  /**
   * Constructor.
   */
  public TemporalEdgeToGellyEdgeWithTimeValue() {
    this.reuseEdge = new org.apache.flink.graph.Edge<>();
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, Tuple2<Long, Long>> map(E temporalEdge) {
    reuseEdge.setSource(temporalEdge.getSourceId());
    reuseEdge.setTarget(temporalEdge.getTargetId());
    reuseEdge.setValue(temporalEdge.getValidTime());
    return reuseEdge;
  }
}
