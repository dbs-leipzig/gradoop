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

package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Associate each graph id in teh hypervertices' heads
 * to the merged vertices
 */
public class CoGroupGraphHeadToVertex
  implements MapFunction<GraphHead, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable tuple to be returned as a result
   */
  private final Tuple2<Vertex, GradoopId> reusable;

  /**
   * Default constructor
   */
  public CoGroupGraphHeadToVertex() {
    reusable = new Tuple2<>();
    reusable.f0 = new Vertex();
  }

  @Override
  public Tuple2<Vertex, GradoopId> map(GraphHead hid) throws Exception {
    reusable.f0.setId(GradoopId.get());
    reusable.f0.setLabel(hid.getLabel());
    reusable.f0.setProperties(hid.getProperties());
    reusable.f1 = hid.getId();
    return reusable;
  }
}
