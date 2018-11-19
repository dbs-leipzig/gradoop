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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;

/**
 * Initializes a {@link Vertex} from a {@link TemporalVertex} instance by discarding the temporal
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties;graphIds")
public class VertexFromTemporal implements MapFunction<TemporalVertex, Vertex> {

  /**
   * Used to reduce instantiations
   */
  private Vertex reuse;

  /**
   * Creates an instance of the VertexFromTemporal map function
   */
  public VertexFromTemporal() {
    this.reuse = new Vertex();
  }

  @Override
  public Vertex map(TemporalVertex value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setProperties(value.getProperties());
    reuse.setGraphIds(value.getGraphIds());
    return reuse;
  }
}
