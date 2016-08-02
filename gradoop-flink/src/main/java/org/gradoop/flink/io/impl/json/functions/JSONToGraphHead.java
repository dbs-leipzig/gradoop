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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Reads graph data from a json document. The document contains at least
 * the graph id, an embedded data document and an embedded meta document.
 * The data document contains all key-value pairs stored at the graphs, the
 * meta document contains the graph label and the vertex/edge identifiers
 * of vertices/edges contained in that graph.
 * <p>
 * Example:
 * <p>
 * {
 * "id":0,
 * "data":{"title":"Graph Databases"},
 * "meta":{"label":"Community","vertices":[0,1,2],"edges":[4,5,6]}
 * }
 */
public class JSONToGraphHead extends JSONToEntity
  implements MapFunction<String, GraphHead> {

  /**
   * Creates graph data objects
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Creates map function
   *
   * @param graphHeadFactory graph data factory
   */
  public JSONToGraphHead(GraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * Creates graph data from JSON string representation.
   *
   * @param s json string representation
   * @return Subgraph storing graph data
   * @throws Exception
   */
  @Override
  public GraphHead map(String s) throws Exception {
    JSONObject jsonGraph = new JSONObject(s);
    GradoopId graphID = getID(jsonGraph);
    String label = getLabel(jsonGraph);
    PropertyList properties = PropertyList.createFromMap(
      getProperties(jsonGraph));

    return graphHeadFactory.initGraphHead(graphID, label, properties);
  }
}
