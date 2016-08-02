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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.json.JSONConstants;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Reads edge data from a json document. The document contains at least
 * the edge id, the source vertex id, the target vertex id, an embedded
 * data document and an embedded meta document.
 * The data document contains all key-value pairs stored at the edge, the
 * meta document contains the edge label and an optional list of graph
 * identifiers the edge is contained in.
 * <p>
 * Example:
 * <p>
 * {
 * "id":0,"start":15,"end":12,
 * "data":{"since":2015},
 * "meta":{"label":"worksFor","graphs":[1,2,3,4]}
 * }
 */
public class JSONToEdge extends JSONToEntity
  implements MapFunction<String, Edge> {
  /**
   * Edge data factory.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates map function.
   *
   * @param edgeFactory edge data factory
   */
  public JSONToEdge(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * Creates an edge from JSON string representation.
   *
   * @param s json string
   * @return Gelly edge storing gradoop edge data
   * @throws Exception
   */
  @Override
  public Edge map(String s) throws Exception {
    JSONObject jsonEdge = new JSONObject(s);
    GradoopId edgeID = getID(jsonEdge);
    String edgeLabel = getLabel(jsonEdge);
    GradoopId sourceID = getSourceId(jsonEdge);
    GradoopId targetID = getTargetId(jsonEdge);
    PropertyList properties = PropertyList.createFromMap(
      getProperties(jsonEdge));
    GradoopIdSet graphs = getGraphs(jsonEdge);

    return edgeFactory.initEdge(edgeID, edgeLabel, sourceID, targetID,
      properties, graphs);
  }

  /**
   * Reads the source vertex identifier from the json object.
   *
   * @param jsonEdge json string representation
   * @return source vertex identifier
   * @throws JSONException
   */
  private GradoopId getSourceId(JSONObject jsonEdge) throws JSONException {
    return GradoopId.fromString(jsonEdge.getString(JSONConstants.EDGE_SOURCE));
  }

  /**
   * Reads the target vertex identifier from the json object.
   *
   * @param jsonEdge json string representation
   * @return target vertex identifier
   * @throws JSONException
   */
  private GradoopId getTargetId(JSONObject jsonEdge) throws JSONException {
    return GradoopId.fromString(jsonEdge.getString(JSONConstants.EDGE_TARGET));
  }
}
