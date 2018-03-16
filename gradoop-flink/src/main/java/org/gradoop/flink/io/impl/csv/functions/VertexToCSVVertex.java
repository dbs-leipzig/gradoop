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
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;

/**
 * Converts an {@link Vertex} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class VertexToCSVVertex extends ElementToCSV<Vertex, CSVVertex> {
  /**
   * Reduce object instantiations.
   */
  private final CSVVertex csvVertex = new CSVVertex();

  @Override
  public CSVVertex map(Vertex vertex) throws Exception {
    csvVertex.setId(vertex.getId().toString());
    csvVertex.setLabel(vertex.getLabel());
    csvVertex.setProperties(getPropertyString(vertex));
    return csvVertex;
  }
}
