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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

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
   *
   * @param <V> EPGM vertex type
   */
  public static class VertexTextFormatter<V extends EPGMVertex> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<V> {

    /**
     * Creates a JSON string representation from a given vertex.
     *
     * @param v vertex
     * @return JSON string representation
     */
    @Override
    public String format(V v) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, v.getId());
        json.put(DATA, writeProperties(v));
        json.put(META, writeGraphElementMeta(v));
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
   *
   * @param <E> EPGM edge type
   */
  public static class EdgeTextFormatter<E extends EPGMEdge> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<E> {

    /**
     * Creates a JSON string representation from a given edge.
     *
     * @param e edge
     * @return JSON string representation
     */
    @Override
    public String format(E e) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, e.getId());
        json.put(EDGE_SOURCE, e.getSourceId());
        json.put(EDGE_TARGET, e.getTargetId());
        json.put(DATA, writeProperties(e));
        json.put(META, writeGraphElementMeta(e));
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
   *
   * @param <G> EPGM graph head type
   */
  public static class GraphTextFormatter<G extends EPGMGraphHead> extends
    EntityToJsonFormatter implements
    TextOutputFormat.TextFormatter<G> {

    /**
     * Creates a JSON string representation of a given graph head object.
     *
     * @param g graph head
     * @return JSON string representation
     */
    @Override
    public String format(G g) {
      JSONObject json = new JSONObject();
      try {
        json.put(IDENTIFIER, g.getId());
        json.put(DATA, writeProperties(g));
        json.put(META, writeGraphMeta(g));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json.toString();
    }
  }
}
