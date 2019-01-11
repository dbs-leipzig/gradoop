/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.impl.json.functions;

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
import java.util.Objects;

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
 * "name":"Alice","gender":"female","age":42
 * }}</pre>
 */
public class MinimalJsonToVertex implements MapFunction<String, Vertex> {

  /**
   * The default label used for the newly created vertices.
   */
  public static final String JSON_VERTEX_LABEL = "JsonRowVertex";

  /**
   * Reduce object instantiations.
   */
  private final Vertex reuse;

  /**
   * Creates a new instance of this JSON string to vertex converting function.
   *
   * @param vertexFactory The vertex factory used to create new vertices.
   */
  public MinimalJsonToVertex(EPGMVertexFactory<Vertex> vertexFactory) {
    this.reuse = Objects.requireNonNull(vertexFactory).createVertex(JSON_VERTEX_LABEL);
    this.reuse.setProperties(Properties.create());
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

    Properties properties = reuse.getProperties();
    if (properties == null) {
      properties = Properties.create();
      reuse.setProperties(properties);
    }
    properties.clear();

    for (Iterator it = jsonVertex.keys(); it.hasNext();) {
      String key = (String) it.next();

      PropertyValue propertyValue = getPropertyValue(jsonVertex, key);
      properties.set(key, propertyValue);
    }
    return reuse;
  }

  /**
   * Convert a value of a JSON object to a {@link PropertyValue}.
   *
   * @param jsonObject The JSON object.
   * @param key        The key associated with the value to convert into a property value.
   * @return The converted value.
   */
  private PropertyValue getPropertyValue(JSONObject jsonObject, String key) {
    PropertyValue propertyValue = new PropertyValue();
    if (jsonObject.optJSONArray(key) != null) {
      JSONArray array = jsonObject.optJSONArray(key);
      // is it list or object?
      boolean listOrObject = array.optJSONObject(0) != null;

      List<PropertyValue> propertyList = new ArrayList<>();

      for (int i = 0; i < array.length(); i++) {
        String stringValue = listOrObject ? array.optJSONObject(i).toString() : array.optString(i);

        PropertyValue listElement = PropertyValue.create(stringValue);
        propertyList.add(listElement);
        propertyValue.setList(propertyList);
      }
    } else if (jsonObject.optString(key) != null) {
      String text = jsonObject.optString(key);
      propertyValue.setString(text);
    } else {
      propertyValue.setObject(null);
    }
    return propertyValue;
  }
}
