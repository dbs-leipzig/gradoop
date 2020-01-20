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
package org.gradoop.dataintegration.importer.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Create an ImportVertex for each tuple of an id and properties.
 *
 * @param <V> the vertex type
 */
@FunctionAnnotation.ForwardedFields("*->properties")
public class PropertiesToVertex<V extends Vertex> implements MapFunction<Properties, V> {
  /**
   * Reduce object instantiations.
   */
  private V vertex;

  /**
   * Create a new CreateImportVertexCSV function
   *
   * @param vertexFactory the factory that is responsible for creating a vertex
   */
  public PropertiesToVertex(VertexFactory<V> vertexFactory) {
    this.vertex = vertexFactory.createVertex();
  }

  @Override
  public V map(final Properties value) {
    vertex.setProperties(value);
    vertex.setId(GradoopId.get());
    return vertex;
  }
}
