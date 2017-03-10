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

package org.gradoop.flink.model.impl.nested.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.tuples.Quad;

/**
 * Uses the join for two different semantic purposes, bot returning the new vertices and
 * updating the old edges
 */
public class AssociateAndMark implements
  JoinFunction<Tuple2<GradoopId, GradoopId>,
               Tuple2<GradoopId, GradoopId>,
               Quad
              > {

  private final Quad q;

  public AssociateAndMark() {
    q = new Quad();
  }

  @Override
  public Quad join(Tuple2<GradoopId, GradoopId> fromDataLake,
                   Tuple2<GradoopId, GradoopId> fromGraphCollection) throws Exception {
    q.update(fromDataLake,fromGraphCollection);
    q.setGCAsVertexIdAndStoreGCInf4();
    return q;
  }
}
