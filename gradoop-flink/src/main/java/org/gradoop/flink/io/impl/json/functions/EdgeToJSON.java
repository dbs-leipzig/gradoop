
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
