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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.flink.model.api.functions.timeextractors.TimestampExtractor;

/**
 * Map function to extract temporal information from an EPGM graph head to create a temporal graph
 * head with a timestamp as the beginning of the elements validity.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class GraphHeadTimestampExtractor implements MapFunction<GraphHead, TemporalGraphHead> {
  /**
   * The user defined timestamp extractor.
   */
  private TimestampExtractor<GraphHead> timestampExtractor;

  /**
   * Creates a new graph head timestamp extractor instance for internal use.
   *
   * @param timestampExtractor the user defined timestamp extractor
   */
  public GraphHeadTimestampExtractor(TimestampExtractor<GraphHead> timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  /**
   * Creates a temporal graph head from an EPGM graph head instance using the provided timestamp
   * extractor to set validFrom timestamp.
   *
   * @param graphHead the EPGM graph head instance to use
   * @return a temporal graph head instance with time information provided by the timestampExtractor
   */
  @Override
  public TemporalGraphHead map(GraphHead graphHead) throws Exception {
    TemporalGraphHead temporalVertex = TemporalGraphHead.fromNonTemporalGraphHead(graphHead);
    temporalVertex.setValidFrom(timestampExtractor.getValidFrom(graphHead));
    return temporalVertex;
  }
}
