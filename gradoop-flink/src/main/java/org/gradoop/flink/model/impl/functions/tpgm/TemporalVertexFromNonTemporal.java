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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;

/**
 * Initializes a {@link TemporalVertex} from a {@link Vertex} instance by setting either
 * default temporal information or, if a timeIntervalExtractor is given, by the extracted time
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties;graphIds")
public class TemporalVertexFromNonTemporal implements MapFunction<Vertex, TemporalVertex> {
  /**
   * The user defined interval extractor, might be {@code null}.
   */
  private TimeIntervalExtractor<Vertex> timeIntervalExtractor;
  /**
   * Reuse this instance to reduce instantiations.
   */
  private TemporalVertex reuse;

  /**
   * Creates an instance of the TemporalVertexFromNonTemporal map function. The temporal instance
   * will have default temporal information.
   */
  public TemporalVertexFromNonTemporal() {
    this.reuse = TemporalVertex.createVertex();
  }

  /**
   * Creates an instance of the TemporalVertexFromNonTemporal map function. The temporal instance
   * will have valid times extracted from the non-temporal instance by the given
   * timeIntervalExtractor.
   *
   * @param timeIntervalExtractor the extractor instance fetches the validFrom and validTo values
   */
  public TemporalVertexFromNonTemporal(TimeIntervalExtractor<Vertex> timeIntervalExtractor) {
    this();
    this.timeIntervalExtractor = timeIntervalExtractor;
  }

  /**
   * Creates a temporal vertex instance from the non-temporal. Id's, label and properties will
   * be kept. If a timeIntervalExtractor is given, the valid time interval will be set with the
   * extracted information.
   *
   * @param value the non-temporal element
   * @return the temporal element
   */
  @Override
  public TemporalVertex map(Vertex value) {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    if (timeIntervalExtractor != null) {
      reuse.setValidFrom(timeIntervalExtractor.getValidFrom(value));
      reuse.setValidTo(timeIntervalExtractor.getValidTo(value));
    }
    return reuse;
  }
}
