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

package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Left join, return first field of left tuple 2.
 *
 * @param <O> an object type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class CreateTuple2WithLong<O> implements
  MapFunction<O, Tuple2<O, Long>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<O, Long> reuseTuple = new Tuple2<>();

  /**
   * Creates this mapper
   *
   * @param secondField user defined long value
   */
  public CreateTuple2WithLong(Long secondField) {
    this.reuseTuple.f1 = secondField;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<O, Long> map(O o) throws Exception {
    reuseTuple.f0 = o;
    return reuseTuple;
  }
}
