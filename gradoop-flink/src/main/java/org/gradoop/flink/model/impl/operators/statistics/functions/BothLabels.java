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
