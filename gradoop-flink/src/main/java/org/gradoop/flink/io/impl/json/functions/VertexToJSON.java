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

import org.apache.flink.api.java.io.TextOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.io.impl.json.JSONConstants;
import org.gradoop.common.model.impl.pojo.Vertex;

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
public class VertexToJSON<V extends Vertex>
  extends EntityToJSON
  implements TextOutputFormat.TextFormatter<V> {

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
      json.put(JSONConstants.IDENTIFIER, v.getId());
      json.put(JSONConstants.DATA, writeProperties(v));
      json.put(JSONConstants.META, writeGraphElementMeta(v));
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return json.toString();
  }
}
