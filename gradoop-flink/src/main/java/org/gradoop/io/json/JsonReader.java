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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.Subgraph;

import java.util.Map;
import java.util.Set;

/**
 * Used to convert json documents into vertices, edges and graphs.
 */
public class JsonReader extends JsonIO {

  /**
   * Reads vertex data from a json document. The document contains at least
   * the vertex id, an embedded data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the vertex, the
   * meta document contains the vertex label and an optional list of graph
   * identifiers the vertex is contained in.
   *
   * Example:
   *
   * {
   * "id":0,
   * "data":{"name":"Alice","gender":"female","age":42},
   * "meta":{"label":"Employee","graphs":[0,1,2,3]}
   * }
   */
  public static class JsonToVertexMapper extends JsonToEntityMapper implements
    MapFunction<String, Vertex<Long, EPFlinkVertexData>> {

    @Override
    public Vertex<Long, EPFlinkVertexData> map(String s) throws Exception {
      JSONObject jsonVertex = new JSONObject(s);
      Long vertexID = getID(jsonVertex);
      String label = getLabel(jsonVertex);
      Map<String, Object> properties = getProperties(jsonVertex);
      Set<Long> graphs = getGraphs(jsonVertex);

      return new Vertex<>(vertexID,
        new EPFlinkVertexData(vertexID, label, properties, graphs));
    }
  }

  /**
   * Reads edge data from a json document. The document contains at least
   * the edge id, the source vertex id, the target vertex id, an embedded
   * data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the edge, the
   * meta document contains the edge label and an optional list of graph
   * identifiers the edge is contained in.
   *
   * Example:
   *
   * {
   * "id":0,"start":15,"end":12,
   * "data":{"since":2015},
   * "meta":{"label":"worksFor","graphs":[1,2,3,4]}
   * }
   */
  public static class JsonToEdgeMapper extends JsonToEntityMapper implements
    MapFunction<String, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public Edge<Long, EPFlinkEdgeData> map(String s) throws Exception {
      JSONObject jsonEdge = new JSONObject(s);
      Long edgeID = getID(jsonEdge);
      String edgeLabel = getLabel(jsonEdge);
      Long sourceID = getSourceVertexID(jsonEdge);
      Long targetID = getTargetVertexID(jsonEdge);
      Map<String, Object> properties = getProperties(jsonEdge);
      Set<Long> graphs = getGraphs(jsonEdge);

      return new Edge<>(sourceID, targetID,
        new EPFlinkEdgeData(edgeID, edgeLabel, sourceID, targetID, properties,
          graphs));
    }

    private Long getSourceVertexID(JSONObject jsonEdge) throws JSONException {
      return jsonEdge.getLong(EDGE_SOURCE);
    }

    private Long getTargetVertexID(JSONObject jsonEdge) throws JSONException {
      return jsonEdge.getLong(EDGE_TARGET);
    }
  }

  /**
   * Reads graph data from a json document. The document contains at least
   * the graph id, an embedded data document and an embedded meta document.
   * The data document contains all key-value pairs stored at the graphs, the
   * meta document contains the graph label.
   *
   * Example:
   *
   * {
   * "id":0,
   * "data":{"title":"Graph Databases"},
   * "meta":{"label":"Community"}
   * }
   */
  public static class JsonToGraphMapper extends JsonToEntityMapper implements
    MapFunction<String, Subgraph<Long, EPFlinkGraphData>> {

    @Override
    public Subgraph<Long, EPFlinkGraphData> map(String s) throws Exception {
      JSONObject jsonGraph = new JSONObject(s);
      Long graphID = getID(jsonGraph);
      String label = getLabel(jsonGraph);
      Map<String, Object> properties = getProperties(jsonGraph);

      return new Subgraph<>(graphID,
        new EPFlinkGraphData(graphID, label, properties));
    }
  }
}
