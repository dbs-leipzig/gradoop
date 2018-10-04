/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.deprecated.json.functions;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.io.impl.deprecated.json.JSONConstants;
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
