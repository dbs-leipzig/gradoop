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

package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 * Associates the old vertices with the new vertex fused ids.
 *
 * Created by Giacomo Bergami on 17/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class CoGroupAssociateOldVerticesWithNewIds implements
  CoGroupFunction<Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<Vertex, GradoopId> reusable;

  /**
   * Reusable collection for traversing the second operand
   */
  private final Collection<GradoopId> reusableList;

  /**
   * Default constructor
   */
  public CoGroupAssociateOldVerticesWithNewIds() {
    reusable = new Tuple2<>();
    reusableList = Lists.newArrayList();
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, GradoopId>> first,
    Iterable<Tuple2<Vertex, GradoopId>> second, Collector<Tuple2<Vertex, GradoopId>> out) throws
    Exception {
    reusableList.clear();
    second.forEach(x -> reusableList.add(x.f0.getId()));
    for (Tuple2<Vertex, GradoopId> x : first) {
      for (GradoopId y : reusableList) {
        reusable.f0 = x.f0;
        reusable.f1 = y;
        out.collect(reusable);
      }
    }
  }
}
