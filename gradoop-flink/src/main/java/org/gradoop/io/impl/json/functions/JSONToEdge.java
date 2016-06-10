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

package org.gradoop.io.impl.json.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.io.impl.json.JSONConstants;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

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
 *
 * @param <E> EPGM edge type
 */
public class JSONToEdge<E extends EPGMEdge>
  extends JSONToEntity
  implements MapFunction<String, E> {
  /**
   * Edge data factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates map function.
   *
   * @param edgeFactory edge data factory
   */
  public JSONToEdge(EPGMEdgeFactory<E> edgeFactory) {
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
  public E map(String s) throws Exception {
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
  private GradoopId getSourceId(JSONObject jsonEdge
  ) throws JSONException {

    return GradoopId.fromString(jsonEdge.getString(JSONConstants.EDGE_SOURCE));
  }

  /**
   * Reads the target vertex identifier from the json object.
   *
   * @param jsonEdge json string representation
   * @return target vertex identifier
   * @throws JSONException
   */
  private GradoopId getTargetId(JSONObject jsonEdge
  ) throws JSONException {

    return GradoopId.fromString(jsonEdge.getString(JSONConstants.EDGE_TARGET));
  }
}
