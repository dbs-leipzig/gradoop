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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Initializes a {@link TemporalEdge} from a {@link Edge} instance by setting either
 * default temporal information or, if a timeIntervalExtractor is given, by the extracted time information.
 *
 * @param <E> The (non-temporal) edge type.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;label;properties;graphIds")
public class EdgeToTemporalEdge<E extends Edge> implements MapFunction<E, TemporalEdge> {
  /**
   * The user defined timestamp extractor.
   */
  private TimeIntervalExtractor<E> timeIntervalExtractor;
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
  public EdgeToTemporalEdge(EdgeFactory<TemporalEdge> elementFactory) {
    this.reuse = elementFactory.createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
  }

  /**
   * Creates an instance of the TemporalEdgeFromNonTemporal map function. The temporal instance
   * will have valid times extracted from the non-temporal instance by the given timeIntervalExtractor.
   *
   * @param elementFactory factory that is responsible for creating a temporal edge instance
   * @param timeIntervalExtractor the extractor instance fetches the validFrom and validTo values
   */
  public EdgeToTemporalEdge(
    EdgeFactory<TemporalEdge> elementFactory,
    TimeIntervalExtractor<E> timeIntervalExtractor) {
    this(elementFactory);
    this.timeIntervalExtractor = timeIntervalExtractor;
  }

  @Override
  public TemporalEdge map(E value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setSourceId(value.getSourceId());
    reuse.setTargetId(value.getTargetId());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    if (timeIntervalExtractor != null) {
      reuse.setValidTime(timeIntervalExtractor.map(value));
    }
    return reuse;
  }
}
