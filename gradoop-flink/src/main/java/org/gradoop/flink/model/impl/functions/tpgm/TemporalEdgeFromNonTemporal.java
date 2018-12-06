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
package org.gradoop.flink.model.impl.functions.tpgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.flink.model.api.functions.TimeIntervalExtractor;

/**
 * Initializes a {@link TemporalEdge} from a {@link Edge} instance by setting either
 * default temporal information or, if a timeIntervalExtractor is given, by the extracted time
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;label;properties;graphIds")
public class TemporalEdgeFromNonTemporal implements MapFunction<Edge, TemporalEdge> {
  /**
   * The user defined timestamp extractor.
   */
  private TimeIntervalExtractor<Edge> timeIntervalExtractor;
  /**
   * Reuse this instance to reduce instantiations.
   */
  private TemporalEdge reuse;

  /**
   * Creates an instance of the TemporalEdgeFromNonTemporal map function. The temporal instance
   * will have default temporal information.
   *
   * @param elementFactory factory that is responsible for creating a temporal edge instance
   */
  public TemporalEdgeFromNonTemporal(EPGMEdgeFactory<TemporalEdge> elementFactory) {
    this.reuse = elementFactory.createEdge(GradoopId.get(), GradoopId.get());
  }

  /**
   * Creates an instance of the TemporalEdgeFromNonTemporal map function. The temporal instance
   * will have valid times extracted from the non-temporal instance by the given
   * timeIntervalExtractor.
   *
   * @param elementFactory factory that is responsible for creating a temporal edge instance
   * @param timeIntervalExtractor the extractor instance fetches the validFrom and validTo values
   */
  public TemporalEdgeFromNonTemporal(
    EPGMEdgeFactory<TemporalEdge> elementFactory,
    TimeIntervalExtractor<Edge> timeIntervalExtractor) {
    this(elementFactory);
    this.timeIntervalExtractor = timeIntervalExtractor;
  }

  /**
   * Creates a temporal edge instance from the non-temporal. Id's, label and properties will
   * be kept. If a timeIntervalExtractor is given, the valid time interval will be set with the
   * extracted information.
   *
   * @param value the non-temporal element
   * @return the temporal element
   */
  @Override
  public TemporalEdge map(Edge value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setSourceId(value.getSourceId());
    reuse.setTargetId(value.getTargetId());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    if (timeIntervalExtractor != null) {
      reuse.setValidFrom(timeIntervalExtractor.getValidFrom(value));
      reuse.setValidTo(timeIntervalExtractor.getValidTo(value));
    }
    return reuse;
  }
}
