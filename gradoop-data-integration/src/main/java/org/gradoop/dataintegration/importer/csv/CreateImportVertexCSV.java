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
package org.gradoop.dataintegration.importer.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * (vertexId, properties) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 * f1: vertexProperties
 *
 * @param <K> id type
 */
public class CreateImportVertexCSV<K extends Comparable<K>>
  implements MapFunction<Tuple2<K, Properties>, ImportVertex<K>> {

  /**
   * Used ImportVertex
   */
  private ImportVertex<K> vertex;
  /**
   * Constructor
   */
  public CreateImportVertexCSV() {
    this.vertex = new ImportVertex<>();
  }

  @Override
  public ImportVertex<K> map(final Tuple2<K, Properties> value) throws Exception {
    vertex.setId(value.f0);
    vertex.setLabel(GradoopConstants.DEFAULT_VERTEX_LABEL);
    vertex.setProperties(value.f1);
    return vertex;
  }
}
