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

package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * replaces a component id by a new graph id
 */
@FunctionAnnotation.ForwardedFields("f1->f1")
public class ComponentToNewBtgId implements MapFunction
  <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public Tuple2<GradoopId, GradoopIdSet> map(
    Tuple2<GradoopId, GradoopIdSet> pair) throws Exception {

    return new Tuple2<>(GradoopId.get(), pair.f1);
  }
}
