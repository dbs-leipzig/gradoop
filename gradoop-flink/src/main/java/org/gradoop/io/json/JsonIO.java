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

package org.gradoop.io.json;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.Attributed;
import org.gradoop.model.GraphElement;
import org.gradoop.model.Labeled;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class JsonIO {
  /**
   * Key for vertex, edge and graph id.
   */
  protected static final String IDENTIFIER = "id";
  /**
   * Key for meta Json object.
   */
  protected static final String META = "meta";
  /**
   * Key for data Json object.
   */
  protected static final String DATA = "data";
  /**
   * Key for vertex, edge and graph label.
   */
  protected static final String LABEL = "label";
  /**
   * Key for graph identifiers at vertices and edges.
   */
  protected static final String GRAPHS = "graphs";
  /**
   * Key for vertex identifiers at graphs.
   */
  protected static final String VERTICES = "vertices";
  /**
   * Key for edge identifiers at graphs.
   */
  protected static final String EDGES = "edges";
  /**
   * Key for edge source vertex id.
   */
  protected static final String EDGE_SOURCE = "source";
  /**
   * Key for edge target vertex id.
   */
  protected static final String EDGE_TARGET = "target";

  /**
   * Contains methods used by all entity readers (e.g. read label, properties).
   */
  protected abstract static class JsonToEntityMapper {

    // necessary for flink UDF initialization
    public JsonToEntityMapper() {
    }

    protected Long getID(JSONObject object) throws JSONException {
      return object.getLong(IDENTIFIER);
    }

    protected String getLabel(JSONObject object) throws JSONException {
      return object.getJSONObject(META).getString(LABEL);
    }

    protected Map<String, Object> getProperties(JSONObject object) throws
      JSONException {
      Map<String, Object> props =
        Maps.newHashMapWithExpectedSize(object.length() * 2);
      object = object.getJSONObject(DATA);
      Iterator<?> keys = object.keys();
      while (keys.hasNext()) {
        String key = keys.next().toString();
        Object o = object.get(key);
        props.put(key, o);
      }
      return props;
    }

    protected Set<Long> getGraphs(JSONObject object) throws JSONException {
      Set<Long> result;
      if (!object.getJSONObject(META).has(GRAPHS)) {
        result = Sets.newHashSetWithExpectedSize(0);
      } else {
        result =
          getArrayValues(object.getJSONObject(META).getJSONArray(GRAPHS));
      }
      return result;
    }

    protected Set<Long> getArrayValues(JSONArray array) throws JSONException {
      Set<Long> result = Sets.newHashSetWithExpectedSize(array.length());
      for (int i = 0; i < array.length(); i++) {
        result.add(array.getLong(i));
      }
      return result;
    }
  }

  protected abstract static class EntityToJsonFormatter {

    protected JSONObject writeProperties(Attributed entity) throws
      JSONException {
      JSONObject data = new JSONObject();
      if (entity.getPropertyCount() > 0) {
        for (String propertyKey : entity.getPropertyKeys()) {
          data.put(propertyKey, entity.getProperty(propertyKey));
        }
      }
      return data;
    }

    protected JSONObject writeMeta(Labeled entity) throws JSONException {
      JSONObject meta = new JSONObject();
      meta.put(LABEL, entity.getLabel());
      return meta;
    }

    protected <T extends Labeled & GraphElement> JSONObject
    writeGraphElementMeta(
      T entity) throws JSONException {
      JSONObject meta = writeMeta(entity);
      meta.put(GRAPHS, getGraphsArray(entity));
      return meta;
    }

    private JSONArray getGraphsArray(final GraphElement graphElement) throws
      JSONException {
      JSONArray graphArray = new JSONArray();
      for (Long graph : graphElement.getGraphs()) {
        graphArray.put(graph);
      }
      return graphArray;
    }
  }
}
