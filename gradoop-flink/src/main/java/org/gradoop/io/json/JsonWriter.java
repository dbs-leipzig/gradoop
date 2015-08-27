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

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.Subgraph;

/**
 * Used to convert vertices, edges and graphs into json documents.
 */
public class JsonWriter extends JsonIO {
  /**
   * Converts a vertex into the following format:
   * <p>
   * {
   * "id":0,
   * "data":{"name":"Alice","gender":"female","age":42},
   * "meta":{"label":"Employee","graphs":[0,1,2,3]}
   * }
   */
  public static class VertexTextFormatter<VD extends VertexData> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<Vertex<Long, VD>> {

    /**
     * Creates a JSON string representation of a vertex data object.
     *
     * @param v vertex data
     * @return JSON string representation
     */
    @Override
    public String format(Vertex<Long, VD> v) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, v.getId());
        json.put(DATA, writeProperties(v.getValue()));
        json.put(META, writeGraphElementMeta(v.getValue()));
      } catch (JSONException e) {
        e.printStackTrace();
      }
      return json.toString();
    }
  }

  /**
   * Converts an edge into the following format:
   * <p>
   * {
   * "id":0,
   * "source":1,
   * "target":2,
   * "data":{"since":2015},
   * "meta":{"label":"friendOf","graphs":[0,1,2,3]}
   * }
   */
  public static class EdgeTextFormatter<ED extends EdgeData> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<Edge<Long, ED>> {

    /**
     * Creates a JSON string representation from a given edge data object.
     *
     * @param e edge data object
     * @return JSON string representation
     */
    @Override
    public String format(Edge<Long, ED> e) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, e.getValue().getId());
        json.put(EDGE_SOURCE, e.getSource());
        json.put(EDGE_TARGET, e.getTarget());
        json.put(DATA, writeProperties(e.getValue()));
        json.put(META, writeGraphElementMeta(e.getValue()));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json.toString();
    }
  }

  /**
   * Converts a graph into the following format:
   * <p>
   * {
   * "id":0,
   * "data":{"title":"Graph Community"},
   * "meta":{"label":"Community"}
   * }
   */
  public static class GraphTextFormatter<GD extends GraphData> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<Subgraph<Long, GD>> {

    /**
     * Creates a JSON string representation of a given graph data object.
     *
     * @param g graph data object
     * @return JSON string representation
     */
    @Override
    public String format(Subgraph<Long, GD> g) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, g.getId());
        json.put(DATA, writeProperties(g.getValue()));
        json.put(META, writeGraphMeta(g.getValue()));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json.toString();
    }
  }
}
