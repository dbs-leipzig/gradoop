/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * /**
 * Creates a {@link Vertex} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * vertex-id;vertex-label;value_1|value_2|...|value_n
 */
public class CSVLineToVertex extends CSVLineToElement<Vertex> {
  /**
   * Used to instantiate the vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Constructor
   *
   * @param epgmVertexFactory EPGM vertex factory
   */
  public CSVLineToVertex(EPGMVertexFactory<Vertex> epgmVertexFactory) {
    this.vertexFactory = epgmVertexFactory;
  }

  @Override
  public Vertex map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 3);
    return vertexFactory.initVertex(
      GradoopId.fromString(tokens[0]),
      tokens[1],
      parseProperties(tokens[1], tokens[2]));
  }
}
