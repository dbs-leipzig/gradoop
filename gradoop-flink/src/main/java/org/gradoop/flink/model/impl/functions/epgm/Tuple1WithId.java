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
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * element => (elementId)
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class Tuple1WithId<EL extends Element>
  implements MapFunction<EL, Tuple1<GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple1<GradoopId> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<GradoopId> map(EL element) throws Exception {
    reuseTuple.f0 = element.getId();
    return reuseTuple;
  }
}
