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

package org.gradoop.flink.io.impl.xmlbasedcsv.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.xmlbasedcsv.XMLBasedCSVConstants;

import java.util.Map;

/**
 * Returns a map which assigns the graph heads gradoop id to its key.
 */
public class GraphHeadKeyMap implements GroupReduceFunction<GraphHead, Map<String, GradoopId>> {
  @Override
  public void reduce(Iterable<GraphHead> iterable,
    Collector<Map<String, GradoopId>> collector) throws Exception {
    Map<String, GradoopId> graphHeadMap = Maps.newHashMap();
    //select the key property and store it in the map together with the id
    for (GraphHead graphHead : iterable) {
      graphHeadMap.put(graphHead.getPropertyValue(
        XMLBasedCSVConstants.PROPERTY_KEY_KEY).getString(), graphHead.getId());
    }
    collector.collect(graphHeadMap);
  }
}
