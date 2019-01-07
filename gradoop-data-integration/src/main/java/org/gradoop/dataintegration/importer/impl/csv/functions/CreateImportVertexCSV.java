/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * Create an ImportVertex for each tuple of an id and properties.
 */
public class CreateImportVertexCSV
  implements MapFunction<Properties, Vertex> {

  /**
   * Used ImportVertex
   */
  private Vertex vertex;

  /**
   * Create a new CreateImportVertexCSV function
   */
  public CreateImportVertexCSV() {
    this.vertex = new Vertex();
    vertex.setLabel(GradoopConstants.DEFAULT_VERTEX_LABEL);
  }

  @Override
  public Vertex map(final Properties value) {
    vertex.setId(GradoopId.get());
    vertex.setProperties(value);
    return vertex;
  }
}
