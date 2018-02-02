/**
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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * EPGM vertex to gelly vertex, where value is vertex id
 */
@FunctionAnnotation.ForwardedFields("id->f0;id->f1")
public class ToGellyVertexWithIdValue implements
  MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, GradoopId> reuse =
    new org.apache.flink.graph.Vertex<>();

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, GradoopId> map(Vertex vertex)
      throws Exception {
    GradoopId id = vertex.getId();
    reuse.setId(id);
    reuse.setValue(id);
    return reuse;
  }
}
