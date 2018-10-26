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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;

/**
 * Map function to extract temporal information from an EPGM vertex to create a temporal vertex
 * with a time interval as validity.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties;graphIds")
public class VertexTimeIntervalExtractor implements MapFunction<Vertex, TemporalVertex> {
  /**
   * The user defined interval extractor.
   */
  private TimeIntervalExtractor<Vertex> timeIntervalExtractor;

  /**
   * Creates a new vertex time interval extractor instance for internal use.
   *
   * @param timestampExtractor the user defined time interval extractor
   */
  public VertexTimeIntervalExtractor(TimeIntervalExtractor<Vertex> timestampExtractor) {
    this.timeIntervalExtractor = timestampExtractor;
  }

  /**
   * Creates an temporal vertex from an EPGM edge instance using the provided time interval
   * extractor to set valid from and valid to timestamps.
   *
   * @param vertex the EPGM vertex instance to use
   * @return a temporal vertex instance with time information provided by the timeIntervalExtractor
   */
  @Override
  public TemporalVertex map(Vertex vertex) {
    TemporalVertex temporalVertex = TemporalVertex.fromNonTemporalVertex(vertex);
    temporalVertex.setValidFrom(timeIntervalExtractor.getValidFrom(vertex));
    temporalVertex.setValidTo(timeIntervalExtractor.getValidTo(vertex));
    return temporalVertex;
  }
}
