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
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Objects;

/**
 * Initializes a {@link EPGMVertex} from a {@link TemporalVertex} instance by discarding the temporal
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties;graphIds")
public class TemporalVertexToVertex implements MapFunction<TemporalVertex, EPGMVertex> {

  /**
   * Used to reduce instantiations
   */
  private EPGMVertex reuse;

  /**
   * Creates an new instance of this map function.
   *
   * @param vertexFactory A factory used to create EPGM vertices.
   */
  public TemporalVertexToVertex(VertexFactory<EPGMVertex> vertexFactory) {
    this.reuse = Objects.requireNonNull(vertexFactory).createVertex();
  }

  @Override
  public EPGMVertex map(TemporalVertex vertex) {
    reuse.setId(vertex.getId());
    reuse.setLabel(vertex.getLabel());
    reuse.setProperties(vertex.getProperties());
    reuse.setGraphIds(vertex.getGraphIds());
    return reuse;
  }
}
