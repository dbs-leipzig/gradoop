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
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object,count1),(object,count2) -> (object,count1 + count2)
 *
 * @param <T> object type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class SumCounts<T> implements JoinFunction<WithCount<T>, WithCount<T>, WithCount<T>> {

  @Override
  public WithCount<T> join(WithCount<T> first, WithCount<T> second) throws Exception {
    first.setCount(first.getCount() + second.getCount());
    return first;
  }
}
