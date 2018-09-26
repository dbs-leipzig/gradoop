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
package org.gradoop.flink.io.impl.deprecated.json.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Reads vertex data from a json document. The document contains at least
 * the vertex id, an embedded data document and an embedded meta document.
 * The data document contains all key-value pairs stored at the vertex, the
 * meta document contains the vertex label and an optional list of graph
 * identifiers the vertex is contained in.
 * <p>
 * Example:
 * <p>
 * {
 * "id":0,
 * "data":{"name":"Alice","gender":"female","age":42},
 * "meta":{"label":"Employee", "out-edges":[0,1,2,3], in-edges:[4,5,6,7],
 * "graphs":[0,1,2,3]}
 * }
 */
public class JSONToVertex extends JSONToEntity
  implements MapFunction<String, Vertex> {

  /**
   * Creates vertex data objects.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Creates map function
   *
   * @param epgmVertexFactory vertex data factory
   */
  public JSONToVertex(EPGMVertexFactory<Vertex> epgmVertexFactory) {
    this.vertexFactory = epgmVertexFactory;
  }

  /**
   * Constructs a vertex from a given JSON string representation.
   *
   * @param s json string
   * @return Gelly vertex storing gradoop vertex data
   * @throws Exception
   */
  @Override
  public Vertex map(String s) throws Exception {
    JSONObject jsonVertex = new JSONObject(s);
    GradoopId vertexID = getID(jsonVertex);
    String label = getLabel(jsonVertex);
    Properties properties = Properties.createFromMap(
      getProperties(jsonVertex));
    GradoopIdSet graphs = getGraphs(jsonVertex);

    return vertexFactory.initVertex(vertexID, label, properties, graphs);
  }
}
