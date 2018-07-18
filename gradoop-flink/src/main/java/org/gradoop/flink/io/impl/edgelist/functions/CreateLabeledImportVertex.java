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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.common.util.GradoopConstants;

/**
 * (vertexId, label) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 *
 * @param <K> id type
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CreateLabeledImportVertex<K extends Comparable<K>>
  implements MapFunction<Tuple2<K, String>, ImportVertex<K>> {
  /**
   * reused ImportVertex
   */
  private ImportVertex<K> reuseVertex;
  /**
   * PropertyKey of property value
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey used PropertyKey
   */
  public CreateLabeledImportVertex(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new ImportVertex<>();
    reuseVertex.setLabel(GradoopConstants.DEFAULT_VERTEX_LABEL);
    reuseVertex.setProperties(Properties.createWithCapacity(1));
  }

  @Override
  public ImportVertex<K> map(Tuple2<K, String> inputTuple) throws Exception {
    reuseVertex.setId(inputTuple.f0);
    reuseVertex.getProperties().set(propertyKey, inputTuple.f1);
    return reuseVertex;
  }
}
