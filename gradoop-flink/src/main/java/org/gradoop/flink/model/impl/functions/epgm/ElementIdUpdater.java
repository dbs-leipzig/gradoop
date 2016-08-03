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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;


/**
 * Updates the id of an EPGM element in a Tuple2 by the GradoopId in the
 * second field.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFieldsFirst("graphIds;label;properties")
@FunctionAnnotation.ForwardedFieldsSecond("*->id")
public class ElementIdUpdater<EL extends Element>
  implements MapFunction<Tuple2<EL, GradoopId>, EL> {

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(Tuple2<EL, GradoopId> tuple2) {
    tuple2.f0.setId(tuple2.f1);
    return tuple2.f0;
  }
}
