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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

/**
 * Maps an EPGMEdge to EPGMEdge with its source and targets
 */
public class EdgeWithSourceTarget implements MapFunction<EPGMEdge, Tuple3<EPGMEdge, GradoopId, GradoopId>> {
  /**
   *  Reduce object instantiations
   */
  private Tuple3<EPGMEdge, GradoopId, GradoopId> reuse;

  /**
   * Constructor
   */
  public EdgeWithSourceTarget() {
    reuse = new Tuple3<>();
  }

  @Override
  public Tuple3<EPGMEdge, GradoopId, GradoopId> map(EPGMEdge edge) throws Exception {
    reuse.f0 = edge;
    reuse.f1 = edge.getSourceId();
    reuse.f2 = edge.getTargetId();
    return reuse;
  }
}
