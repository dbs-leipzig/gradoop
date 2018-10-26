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
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;

/**
 * Map function to extract temporal information from an EPGM graph head to create a temporal graph
 * head with a time interval as validity.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class GraphHeadTimeIntervalExtractor implements MapFunction<GraphHead, TemporalGraphHead> {
  /**
   * The user defined interval extractor.
   */
  private TimeIntervalExtractor<GraphHead> timeIntervalExtractor;

  /**
   * Creates a new graph head time interval extractor instance for internal use.
   *
   * @param timestampExtractor the user defined time interval extractor
   */
  public GraphHeadTimeIntervalExtractor(TimeIntervalExtractor<GraphHead> timestampExtractor) {
    this.timeIntervalExtractor = timestampExtractor;
  }

  /**
   * Creates an temporal graph head from an EPGM graph head instance using the provided time
   * interval extractor to set valid from and valid to timestamps.
   *
   * @param graphHead the EPGM graph head instance to use
   * @return a temporal graph head instance with time information provided by the
   *         timeIntervalExtractor
   */
  @Override
  public TemporalGraphHead map(GraphHead graphHead) {
    TemporalGraphHead temporalGraphHead = TemporalGraphHead.fromNonTemporalGraphHead(graphHead);
    temporalGraphHead.setValidFrom(timeIntervalExtractor.getValidFrom(graphHead));
    temporalGraphHead.setValidTo(timeIntervalExtractor.getValidTo(graphHead));
    return temporalGraphHead;
  }
}
