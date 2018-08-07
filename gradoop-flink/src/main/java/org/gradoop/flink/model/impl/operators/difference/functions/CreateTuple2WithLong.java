/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
