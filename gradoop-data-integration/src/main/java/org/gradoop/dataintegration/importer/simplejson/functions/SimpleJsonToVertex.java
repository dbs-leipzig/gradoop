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
package org.gradoop.dataintegration.importer.simplejson.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reads vertex data from a json document. The document contains at least
 * the vertex id, an embedded data document and an embedded meta document.
 * The data document contains all key-value pairs stored at the vertex, the
 * meta document contains the vertex label and an optional list of graph
 * identifiers the vertex is contained in.
 * <p>
 * Example:
 * <p>
 * <pre>{@code {
 * "id":0,
 * "data":{"name":"Alice","gender":"female","age":42},
 * "meta":{"label":"Employee", "out-edges":[0,1,2,3], in-edges:[4,5,6,7],
 * "graphs":[0,1,2,3]}
 * }}</pre>
 */
public class SimpleJsonToVertex implements MapFunction<String, Vertex> {

  /**
   * The default label used for the newly created vertices.
   */
  public static final String DEFAULT_VERTEX_LABEL = "JsonRowVertex";

  /**
   * The factory used to create new vertices.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Creates a new instance of this JSON string to vertex converting function.
   *
   * @param epgmVertexFactory The vertex factory used to create new vertices.
   */
  public SimpleJsonToVertex(EPGMVertexFactory<Vertex> epgmVertexFactory) {
    this.vertexFactory = epgmVertexFactory;
  }

  /**
   * Constructs a vertex from a given JSON string representation.
   *
   * @param jsonString The String representation of a JSON object.
   * @return A new vertex from the JSON object.
   */
  @Override
  public Vertex map(String jsonString) throws Exception {
    JSONObject jsonVertex = new JSONObject(jsonString);

    Vertex vertex = vertexFactory.createVertex(DEFAULT_VERTEX_LABEL);
    Properties properties = Properties.create();

    for (Iterator it = jsonVertex.keys(); it.hasNext();) {
      String key = (String) it.next();

      PropertyValue propertyValue = getPropertyValue(jsonVertex, key);
      properties.set(key, propertyValue);
    }
    vertex.setProperties(properties);
    return vertex;
  }

  /**
   * Convert a value of a JSON object to a {@link PropertyValue}.
   *
   * @param jsonObject The JSON object.
   * @param key        The key associated with the value to convert into a property value.
   * @return The converted value.
   */
  private PropertyValue getPropertyValue(JSONObject jsonObject, String key) {
    PropertyValue pv = new PropertyValue();
    if (jsonObject.optJSONArray(key) != null) {
      JSONArray array = jsonObject.optJSONArray(key);
      // is it list or object?
      boolean listOrObject = array.optJSONObject(0) != null;

      List<PropertyValue> propertyList = new ArrayList<>();

      for (int i = 0; i < array.length(); i++) {
        String valString = listOrObject ? array.optJSONObject(i).toString() : array.optString(i);

        PropertyValue propValue = new PropertyValue();
        propValue.setString(valString);
        propertyList.add(propValue);
        pv.setList(propertyList);
      }
    } else if (jsonObject.optString(key) != null) {
      String text = jsonObject.optString(key);
      pv.setString(text);
    }
    return pv;
  }
}
