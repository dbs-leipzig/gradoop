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
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * turns an EPGM edge into a Gelly edge without data.
 */
public class ToGellyEdgeWithNullValue implements
  MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, NullValue>> {

  /**
   * Reduce instantiations
   */
  private final org.apache.flink.graph.Edge<GradoopId, NullValue> reuse;

  /**
   * Constructor
   */
  public ToGellyEdgeWithNullValue() {
    reuse = new org.apache.flink.graph.Edge<>();
    reuse.f2 = NullValue.getInstance();
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, NullValue> map(Edge e)
      throws Exception {
    reuse.setSource(e.getSourceId());
    reuse.setTarget(e.getTargetId());
    return reuse;
  }
}
