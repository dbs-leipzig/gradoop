/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.json.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;

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
  private final VertexFactory vertexFactory;

  /**
   * Creates map function
   *
   * @param vertexFactory vertex data factory
   */
  public JSONToVertex(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
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
    PropertyList properties = PropertyList.createFromMap(
      getProperties(jsonVertex));
    GradoopIdSet graphs = getGraphs(jsonVertex);

    return vertexFactory.initVertex(vertexID, label, properties, graphs);
  }
}
