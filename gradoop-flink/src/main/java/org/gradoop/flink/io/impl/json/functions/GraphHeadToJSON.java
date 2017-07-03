
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
