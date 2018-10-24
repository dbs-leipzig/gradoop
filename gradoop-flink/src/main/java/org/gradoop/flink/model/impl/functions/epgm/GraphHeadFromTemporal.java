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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;

/**
 * Initializes a {@link GraphHead} from a {@link TemporalGraphHead} instance by discarding the temporal
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class GraphHeadFromTemporal implements MapFunction<TemporalGraphHead, GraphHead> {

  /**
   * The factory to init the EPGM GraphHead
   */
  private GraphHeadFactory factory;

  /**
   * Creates an instance of the GraphHeadFromTemporal map function
   */
  public GraphHeadFromTemporal() {
    this.factory = new GraphHeadFactory();
  }

  @Override
  public GraphHead map(TemporalGraphHead value) throws Exception {
    return factory.initGraphHead(value.getId(), value.getLabel(), value.getProperties());
  }
}
