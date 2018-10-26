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
package org.gradoop.flink.model.impl.functions.timeextractors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;

/**
 * Map function to extract temporal information from an EPGM edge to create a temporal edge
 * with a time interval as its validity.
 */
@FunctionAnnotation.ForwardedFields("id;label;sourceId;targetId;properties;graphIds")
public class EdgeTimeIntervalExtractor implements MapFunction<Edge, TemporalEdge> {
  /**
   * The user defined interval extractor.
   */
  private TimeIntervalExtractor<Edge> timeIntervalExtractor;

  /**
   * Creates a new edge time interval extractor instance for internal use.
   *
   * @param timestampExtractor the user defined time interval extractor
   */
  public EdgeTimeIntervalExtractor(TimeIntervalExtractor<Edge> timestampExtractor) {
    this.timeIntervalExtractor = timestampExtractor;
  }

  /**
   * Creates an temporal edge from an EPGM edge instance using the provided time interval extractor
   * to set valid from and valid to timestamps.
   *
   * @param edge the EPGM edge instance to use
   * @return a temporal edge instance with time information provided by the timeIntervalExtractor
   */
  @Override
  public TemporalEdge map(Edge edge) {
    TemporalEdge temporalEdge = TemporalEdge.fromNonTemporalEdge(edge);
    temporalEdge.setValidFrom(timeIntervalExtractor.getValidFrom(edge));
    temporalEdge.setValidTo(timeIntervalExtractor.getValidTo(edge));
    return temporalEdge;
  }
}
