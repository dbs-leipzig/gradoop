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

import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.io.impl.deprecated.json.JSONConstants;
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
