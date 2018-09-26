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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.deprecated.json.JSONConstants;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

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
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Creates map function.
   *
   * @param epgmEdgeFactory edge data factory
   */
  public JSONToEdge(EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
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
    Properties properties = Properties.createFromMap(
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
