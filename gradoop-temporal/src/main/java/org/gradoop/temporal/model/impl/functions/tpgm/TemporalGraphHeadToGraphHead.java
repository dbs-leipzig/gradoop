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
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

import java.util.Objects;

/**
 * Initializes a {@link EPGMGraphHead} from a {@link TemporalGraphHead} instance by discarding the temporal
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class TemporalGraphHeadToGraphHead implements MapFunction<TemporalGraphHead, EPGMGraphHead> {

  /**
   * Used to reduce instantiations.
   */
  private final EPGMGraphHead reuse;

  /**
   * Creates a new instance of this map function.
   *
   * @param graphHeadFactory A factory used to create EPGM graph heads.
   */
  public TemporalGraphHeadToGraphHead(GraphHeadFactory<EPGMGraphHead> graphHeadFactory) {
    reuse = Objects.requireNonNull(graphHeadFactory).createGraphHead();
  }

  @Override
  public EPGMGraphHead map(TemporalGraphHead graphHead) {
    reuse.setId(graphHead.getId());
    reuse.setLabel(graphHead.getLabel());
    reuse.setProperties(graphHead.getProperties());
    return reuse;
  }
}
