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
package org.gradoop.flink.io.impl.minimal.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * (vertexId, label, properties) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 * f1: vertexLabel
 * f2: vertexProperties
 *
 * @param <K> id type
 */
public class CreateLabeledImportVertexProperties<K extends Comparable<K>>
implements MapFunction<Tuple3<K, String, Properties>, ImportVertex<K>> {

  /**
   * Used ImportVertex
   */
  private ImportVertex<K> vertex;

  /**
   * Constructor
   */
  public CreateLabeledImportVertexProperties() {
    this.vertex = new ImportVertex<>();
  }

  @Override
  public ImportVertex<K> map(Tuple3<K, String, Properties> value) throws Exception {

    vertex.setId(value.f0);
    vertex.setLabel(value.f1);
    vertex.setProperties(value.f2);
    return vertex;
  }
}
