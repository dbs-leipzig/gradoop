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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.json.JSONConstants;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Contains methods used by all entity writers (e.g. write meta, data).
 */
public class EntityToJSON {

  /**
   * Writes all key-value properties to a json object and returns it.
   *
   * @param entity entity with key-value properties
   * @return json object containing the properties
   * @throws JSONException
   */
  protected JSONObject writeProperties(EPGMAttributed entity) throws
    JSONException {
    JSONObject data = new JSONObject();
    if (entity.getPropertyCount() > 0) {
      for (String propertyKey : entity.getPropertyKeys()) {
        data.put(
          propertyKey, entity.getPropertyValue(propertyKey).getObject());
      }
    }
    return data;
  }

  /**
   * Writes all meta data regarding a labeled graph element to a json
   * object and returns it.
   *
   * @param entity labeled graph element (e.g., vertex and edge)
   * @param <T>    input element type
   * @return json object containing meta information
   * @throws JSONException
   */
  protected <T extends EPGMLabeled & EPGMGraphElement> JSONObject
  writeGraphElementMeta(
    T entity) throws JSONException {
    JSONObject meta = writeMeta(entity);
    if (entity.getGraphCount() > 0) {
      meta.put(JSONConstants.GRAPHS, writeJsonArray(entity.getGraphIds()));
    }
    return meta;
  }

  /**
   * Writes all meta data regarding a logical graph to a json object and
   * returns it.
   *
   * @param entity logical graph data
   * @param <T>    graph data type
   * @return json object with graph meta data
   * @throws JSONException
   */
  protected <T extends GraphHead> JSONObject
  writeGraphMeta(T entity) throws JSONException {
    return writeMeta(entity);
  }

  /**
   * Writes meta data (e.g., label) to a json object and returns it.
   *
   * @param entity labeled entity
   * @return json object with meta data containing the label
   * @throws JSONException
   */
  private JSONObject writeMeta(EPGMLabeled entity) throws JSONException {
    JSONObject meta = new JSONObject();
    meta.put(JSONConstants.LABEL, entity.getLabel());
    return meta;
  }

  /**
   * Creates a json array from a given set of identifiers.
   *
   * @param values identifier set
   * @return json array containing the identifiers
   */
  private JSONArray writeJsonArray(final GradoopIdSet values) {
    JSONArray jsonArray = new JSONArray();
    for (GradoopId val : values) {
      jsonArray.put(val);
    }
    return jsonArray;
  }
}

