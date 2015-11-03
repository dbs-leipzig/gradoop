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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;

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
   * <p>
   * Example:
   * <p>
   * {
   * "id":0,
   * "data":{"name":"Alice","gender":"female","age":42},
   * "meta":{"label":"Employee", "out-edges":[0,1,2,3], in-edges:[4,5,6,7],
   * "graphs":[0,1,2,3]}
   * }
   *
   * @param <VD> EPGM vertex type class
   */
  public static class JsonToVertexMapper<VD extends EPGMVertex> extends
    JsonToEntityMapper implements MapFunction<String, VD> {

    /**
     * Creates vertex data objects.
     */
    private final EPGMVertexFactory<VD> vertexFactory;

    /**
     * Creates map function
     *
     * @param vertexFactory vertex data factory
     */
    public JsonToVertexMapper(EPGMVertexFactory<VD> vertexFactory) {
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
    public VD map(String s) throws Exception {
      JSONObject jsonVertex = new JSONObject(s);
      Long vertexID = getID(jsonVertex);
      String label = getLabel(jsonVertex);
      Map<String, Object> properties = getProperties(jsonVertex);
      Set<Long> graphs = getGraphs(jsonVertex);

      return vertexFactory.createVertex(vertexID, label, properties, graphs);
    }
  }

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
   * @param <ED> EPGM edge type
   */
  public static class JsonToEdgeMapper<ED extends EPGMEdge> extends
    JsonToEntityMapper implements MapFunction<String, ED> {

    /**
     * Edge data factory.
     */
    private final EPGMEdgeFactory<ED> edgeFactory;

    /**
     * Creates map function.
     *
     * @param edgeFactory edge data factory
     */
    public JsonToEdgeMapper(EPGMEdgeFactory<ED> edgeFactory) {
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
    public ED map(String s) throws Exception {
      JSONObject jsonEdge = new JSONObject(s);
      Long edgeID = getID(jsonEdge);
      String edgeLabel = getLabel(jsonEdge);
      Long sourceID = getSourceVertexID(jsonEdge);
      Long targetID = getTargetVertexID(jsonEdge);
      Map<String, Object> properties = getProperties(jsonEdge);
      Set<Long> graphs = getGraphs(jsonEdge);

      return edgeFactory.createEdge(edgeID, edgeLabel, sourceID, targetID,
        properties, graphs);
    }

    /**
     * Reads the source vertex identifier from the json object.
     *
     * @param jsonEdge json string representation
     * @return source vertex identifier
     * @throws JSONException
     */
    private Long getSourceVertexID(JSONObject jsonEdge) throws JSONException {
      return jsonEdge.getLong(EDGE_SOURCE);
    }

    /**
     * Reads the target vertex identifier from the json object.
     *
     * @param jsonEdge json string representation
     * @return target vertex identifier
     * @throws JSONException
     */
    private Long getTargetVertexID(JSONObject jsonEdge) throws JSONException {
      return jsonEdge.getLong(EDGE_TARGET);
    }
  }

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
   *
   * @param <GD> EPGM graph head type
   */
  public static class JsonToGraphMapper<GD extends EPGMGraphHead> extends
    JsonToEntityMapper implements MapFunction<String, GD> {

    /**
     * Creates graph data objects
     */
    private final EPGMGraphHeadFactory<GD> graphHeadFactory;

    /**
     * Creates map function
     *
     * @param graphHeadFactory graph data factory
     */
    public JsonToGraphMapper(EPGMGraphHeadFactory<GD> graphHeadFactory) {
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
    public GD map(String s) throws Exception {
      JSONObject jsonGraph = new JSONObject(s);
      Long graphID = getID(jsonGraph);
      String label = getLabel(jsonGraph);
      Map<String, Object> properties = getProperties(jsonGraph);

      return graphHeadFactory.createGraphHead(graphID, label, properties);
    }
  }
}
