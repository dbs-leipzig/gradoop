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
package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 *
 * @param <GE> Vertex, Head or GraphHead information
 */
public class SerializeInGraphInformation<GE extends GraphElement>
  implements MapFunction<GE, String> {
  /**
   * Reusable builder
   */
  private StringBuilder sb;

  /**
   * Default constructor
   */
  public SerializeInGraphInformation() {
    sb = new StringBuilder();
  }

  @Override
  public String map(GE graphElement) throws Exception {
    sb.setLength(0);
    sb.append(graphElement.getId().toString());
    if (graphElement.getGraphIds() != null) {
      Iterator<GradoopId> graphIds = graphElement.getGraphIds().iterator();
      while (graphIds.hasNext()) {
        GradoopId p = graphIds.next();
        sb.append(',').append(p.toString());
      }
    }
    return sb.toString();
  }
}
