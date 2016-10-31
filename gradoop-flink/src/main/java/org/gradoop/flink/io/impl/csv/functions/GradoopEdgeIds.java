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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.io.impl.csv.CSVConstants;

import java.util.HashMap;
import java.util.Map;


public class GradoopEdgeIds extends RichMapFunction<Edge, Edge> {

  public static final String ID_MAP = "idMap";

  private Map<String, GradoopId> map;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    map = getRuntimeContext()
      .<HashMap<String, GradoopId>>getBroadcastVariable(ID_MAP).get(0);
  }

  @Override
  public Edge map(Edge edge) throws Exception {
    PropertyList properties = edge.getProperties();
    //TODO remove properties when it is supportet
    edge.setSourceId(
      map.get(properties.get(CSVConstants.PROPERTY_KEY_SOURCE).getString()));
    edge.setTargetId(
      map.get(properties.get(CSVConstants.PROPERTY_KEY_TARGET).getString()));
    return edge;
  }
}
