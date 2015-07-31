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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.io.reader;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.Subgraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.Map;

public class JsonReader {

  private static final String IDENTIFIER = "id";
  private static final String META = "meta";
  private static final String DATA = "data";

  private static final String CLASS = "class";

  private static final String TYPE = "type";
  private static final String EDGE_SOURCE = "start";
  private static final String EDGE_TARGET = "end";


  private abstract static class JsonToGraphElementMapper {

    // necessary for flink UDF initialization
    public JsonToGraphElementMapper() {}

    protected Map<String, Object> getProperties(JSONObject object) throws
      JSONException {
      Map<String, Object> props = Maps.newHashMap();
      Iterator<?> keys = object.keys();
      while (keys.hasNext()) {
        String key = keys.next().toString();
        Object o = object.get(key);
        props.put(key, o);
      }
      return props;
    }
  }

  public static class JsonToVertexMapper extends
    JsonToGraphElementMapper implements
    MapFunction<String, Vertex<Long, EPFlinkVertexData>> {

    @Override
    public Vertex<Long, EPFlinkVertexData> map(String s) throws Exception {
      JSONObject jsonObject = new JSONObject(s);
      Long vertexID = jsonObject.getLong(IDENTIFIER);
      String label = getLabel(jsonObject);
      Map<String, Object> properties =
        getProperties(jsonObject.getJSONObject(DATA));

      return new Vertex<>(vertexID,
        new EPFlinkVertexData(vertexID, label, properties));
    }

    private String getLabel(JSONObject object) throws JSONException {
      return object.getJSONObject(META).getString(CLASS);
    }
  }

  public static class JsonToEdgeMapper extends
    JsonToGraphElementMapper implements
    MapFunction<String, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public Edge<Long, EPFlinkEdgeData> map(String s) throws Exception {
      JSONObject jsonObject = new JSONObject(s);
      Long edgeID = jsonObject.getLong(IDENTIFIER);
      Long sourceID = jsonObject.getLong(EDGE_SOURCE);
      Long targetID = jsonObject.getLong(EDGE_TARGET);
      String edgeType = getType(jsonObject);
      Map<String, Object> properties =
        getProperties(jsonObject.getJSONObject(DATA));

      return new Edge<>(sourceID, targetID,
        new EPFlinkEdgeData(edgeID, edgeType, sourceID, targetID, properties));
    }

    private String getType(JSONObject object) throws JSONException {
      return object.getJSONObject(META).getString(TYPE);
    }
  }

  public static class JsonToGraphMapper extends
    JsonToGraphElementMapper implements
    MapFunction<String, Subgraph<Long, EPFlinkGraphData>> {

    @Override
    public Subgraph<Long, EPFlinkGraphData> map(String s) throws Exception {
      throw new NotImplementedException();
    }
  }

}
