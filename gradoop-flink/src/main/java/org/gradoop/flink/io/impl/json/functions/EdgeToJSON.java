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
import org.gradoop.common.model.impl.pojo.Edge;

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
public class EdgeToJSON<E extends Edge>
  extends EntityToJSON
  implements TextOutputFormat.TextFormatter<E> {

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
      json.put(JSONConstants.IDENTIFIER, e.getId());
      json.put(JSONConstants.EDGE_SOURCE, e.getSourceId());
      json.put(JSONConstants.EDGE_TARGET, e.getTargetId());
      json.put(JSONConstants.DATA, writeProperties(e));
      json.put(JSONConstants.META, writeGraphElementMeta(e));
    } catch (JSONException ex) {
      ex.printStackTrace();
    }
    return json.toString();
  }
}
