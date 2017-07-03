
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
