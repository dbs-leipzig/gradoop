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

package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * (id,label1),(id,label2) -> (label1,label2)
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class BothLabels implements JoinFunction<IdWithLabel, IdWithLabel, Tuple2<String, String>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<String, String> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<String, String> join(IdWithLabel first, IdWithLabel second) throws Exception {
    reuseTuple.f0 = first.getLabel();
    reuseTuple.f1 = second.getLabel();
    return reuseTuple;
  }
}
