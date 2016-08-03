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
import org.gradoop.common.model.impl.pojo.GraphHead;

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
public class GraphHeadToJSON<G extends GraphHead>
  extends EntityToJSON
  implements TextOutputFormat.TextFormatter<G> {

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
      json.put(JSONConstants.IDENTIFIER, g.getId());
      json.put(JSONConstants.DATA, writeProperties(g));
      json.put(JSONConstants.META, writeGraphMeta(g));
    } catch (JSONException ex) {
      ex.printStackTrace();
    }
    return json.toString();
  }
}
