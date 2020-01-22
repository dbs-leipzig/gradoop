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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

/**
 * Associate each graph id in teh hypervertices' heads
 * to the merged vertices
 */
public class CoGroupGraphHeadToVertex
  implements MapFunction<EPGMGraphHead, Tuple2<EPGMVertex, GradoopId>> {

  /**
   * Reusable tuple to be returned as a result
   */
  private final Tuple2<EPGMVertex, GradoopId> reusable;

  /**
   * Default constructor
   */
  public CoGroupGraphHeadToVertex() {
    reusable = new Tuple2<>();
    reusable.f0 = new EPGMVertex();
  }

  @Override
  public Tuple2<EPGMVertex, GradoopId> map(EPGMGraphHead hid) throws Exception {
    reusable.f0.setId(GradoopId.get());
    reusable.f0.setLabel(hid.getLabel());
    reusable.f0.setProperties(hid.getProperties());
    reusable.f1 = hid.getId();
    return reusable;
  }
}
