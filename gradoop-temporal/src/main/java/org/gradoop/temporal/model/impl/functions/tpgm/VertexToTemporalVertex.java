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
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Initializes a {@link TemporalVertex} from a {@link Vertex} instance by setting either
 * default temporal information or, if a timeIntervalExtractor is given, by the extracted time information.
 *
 * @param <V> The (non-temporal) vertex type.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties;graphIds")
public class VertexToTemporalVertex<V extends Vertex> implements MapFunction<V, TemporalVertex> {
  /**
   * The user defined interval extractor, might be {@code null}.
   */
  private TimeIntervalExtractor<V> timeIntervalExtractor;
  /**
   * Reuse this instance to reduce instantiations.
   */
  private TemporalVertex reuse;

  /**
   * Creates an instance of the TemporalVertexFromNonTemporal map function. The temporal instance
   * will have default temporal information.
   *
   * @param elementFactory factory that is responsible for creating a temporal vertex instance
   */
  public VertexToTemporalVertex(VertexFactory<TemporalVertex> elementFactory) {
    this.reuse = elementFactory.createVertex();
  }

  /**
   * Creates an instance of the TemporalVertexFromNonTemporal map function. The temporal instance
   * will have valid times extracted from the non-temporal instance by the given timeIntervalExtractor.
   *
   * @param elementFactory factory that is responsible for creating a temporal vertex instance
   * @param timeIntervalExtractor the extractor instance fetches the validFrom and validTo values
   */
  public VertexToTemporalVertex(
    VertexFactory<TemporalVertex> elementFactory,
    TimeIntervalExtractor<V> timeIntervalExtractor) {
    this(elementFactory);
    this.timeIntervalExtractor = timeIntervalExtractor;
  }

  @Override
  public TemporalVertex map(V value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    if (timeIntervalExtractor != null) {
      reuse.setValidTime(timeIntervalExtractor.map(value));
    }
    return reuse;
  }
}
