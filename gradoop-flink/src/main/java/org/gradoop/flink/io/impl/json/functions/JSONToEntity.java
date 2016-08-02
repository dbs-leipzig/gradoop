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

import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.io.impl.json.JSONConstants;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Iterator;
import java.util.Map;

/**
 * Contains methods used by all entity readers (e.g. read label, properties).
 */
public class JSONToEntity {

  /**
   * Reads the entity identifier from the json object.
   *
   * @param object json object
   * @return entity identifier
   * @throws JSONException
   */
  protected GradoopId getID(JSONObject object) throws JSONException {
    return GradoopId.fromString(object.getString(JSONConstants.IDENTIFIER));
  }

  /**
   * Reads the entity label from the json object.
   *
   * @param object json object
   * @return entity label
   * @throws JSONException
   */
  protected String getLabel(JSONObject object) throws JSONException {
    return object
      .getJSONObject(JSONConstants.META)
      .getString(JSONConstants.LABEL);
  }

  /**
   * Reads the key-value properties from the json object.
   *
   * @param object json object
   * @return key-value properties
   * @throws JSONException
   */
  protected Map<String, Object> getProperties(JSONObject object) throws
    JSONException {
    Map<String, Object> props =
      Maps.newHashMapWithExpectedSize(object.length() * 2);
    object = object.getJSONObject(JSONConstants.DATA);
    Iterator<?> keys = object.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      Object o = object.get(key);
      props.put(key, o);
    }
    return props;
  }

  /**
   * Reads a set of graph identifiers from the json object.
   *
   * @param object json object
   * @return graph identifiers
   * @throws JSONException
   */
  protected GradoopIdSet getGraphs(JSONObject object) throws JSONException {
    GradoopIdSet result;
    if (!object.getJSONObject(JSONConstants.META).has(JSONConstants.GRAPHS)) {
      result = new GradoopIdSet();
    } else {
      result = getArrayValues(object
          .getJSONObject(JSONConstants.META)
          .getJSONArray(JSONConstants.GRAPHS));
    }
    return result;
  }

  /**
   * Reads a set of Long values from a given json array.
   *
   * @param array json array
   * @return long values
   * @throws JSONException
   */
  protected GradoopIdSet getArrayValues(JSONArray array) throws
    JSONException {

    GradoopIdSet result = new GradoopIdSet();

    for (int i = 0; i < array.length(); i++) {
      result.add(GradoopId.fromString(array.getString(i)));
    }
    return result;
  }
}
