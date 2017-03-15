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

package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Maps each right edge to its id
 */
@FunctionAnnotation.ForwardedFields("f0 -> f0")
public class MapRightEdgeToItsId implements MapFunction<Tuple2<GradoopId, Edge>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable element
   */
  private Tuple2<GradoopId, GradoopId> gid;

  /**
   * Default constructor
   */
  public MapRightEdgeToItsId() {
    gid = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(Tuple2<GradoopId, Edge> value) throws Exception {
    gid.f0 = value.f0;
    gid.f1 = value.f1.getId();
    return gid;
  }
}
