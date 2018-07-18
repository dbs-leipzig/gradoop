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
package org.gradoop.flink.io.impl.edgelist.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * (vertexId) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 *
 * @param <K> comparable key
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CreateImportVertex<K extends Comparable<K>>
  implements MapFunction<Tuple1<K>, ImportVertex<K>> {
  /**
   * Reduce object instantiations
   */
  private final ImportVertex<K> reuseVertex;

  /**
   * Constructor
   */
  public CreateImportVertex() {
    this.reuseVertex = new ImportVertex<>();
    this.reuseVertex.setLabel(GradoopConstants.DEFAULT_VERTEX_LABEL);
  }

  @Override
  public ImportVertex<K> map(Tuple1<K> value) throws Exception {
    reuseVertex.f0 = value.f0;
    return reuseVertex;
  }
}
