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
import org.gradoop.flink.model.api.functions.timeextractors.TimestampExtractor;

/**
 * Map function to extract temporal information from an EPGM edge to create a temporal edge
 * with a timestamp as the beginning of the elements validity.
 */
@FunctionAnnotation.ForwardedFields("id;label;sourceId;targetId;properties;graphIds")
public class EdgeTimestampExtractor implements MapFunction<Edge, TemporalEdge> {
  /**
   * The user defined timestamp extractor.
   */
  private TimestampExtractor<Edge> timestampExtractor;

  /**
   * Creates a new edge timestamp extractor instance for internal use.
   *
   * @param timestampExtractor the user defined timestamp extractor
   */
  public EdgeTimestampExtractor(TimestampExtractor<Edge> timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  /**
   * Creates a temporal edge from an EPGM edge instance using the provided timestamp extractor
   * to set validFrom timestamp.
   *
   * @param edge the EPGM edge instance to use
   * @return a temporal edge instance with time information provided by the timestampExtractor
   */
  @Override
  public TemporalEdge map(Edge edge) throws Exception {
    TemporalEdge temporalVertex = TemporalEdge.fromNonTemporalEdge(edge);
    temporalVertex.setValidFrom(timestampExtractor.getValidFrom(edge));
    return temporalVertex;
  }
}
