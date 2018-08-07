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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Maps an Edge to Edge with its source and targets
 */
public class EdgeWithSourceTarget implements MapFunction<Edge, Tuple3<Edge, GradoopId, GradoopId>> {
  /**
   *  Reduce object instantiations
   */
  private Tuple3<Edge, GradoopId, GradoopId> reuse;

  /**
   * Constructor
   */
  public EdgeWithSourceTarget() {
    reuse = new Tuple3<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<Edge, GradoopId, GradoopId> map(Edge edge) throws Exception {
    reuse.f0 = edge;
    reuse.f1 = edge.getSourceId();
    reuse.f2 = edge.getTargetId();
    return reuse;
  }
}
