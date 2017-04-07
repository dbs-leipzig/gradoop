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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Associates each element to its belonging graph. This belonging
 * is not necessairly stored in the vertex, because all the
 * belongs-to-graph information is kept in the distributed
 * indexing structure, that is the IdGraphDatabase
 *
 * @param <X> GraphElement type
 */
@FunctionAnnotation.ForwardedFields("id -> f1")
public class AssociateElementToIdAndGraph<X extends GraphElement> implements
  FlatMapFunction<X, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable lement
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructor
   */
  public AssociateElementToIdAndGraph() {
    reusable = new Tuple2<>();
  }

  @Override
  public void flatMap(X value, Collector<Tuple2<GradoopId, GradoopId>> out) throws Exception {
    reusable.f1 = value.getId();
    for (GradoopId gid : value.getGraphIds()) {
      reusable.f0 = gid;
      out.collect(reusable);
    }
  }
}
