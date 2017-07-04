/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (f0,f1,f2) => f0
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 * @param <T3> f3 type
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class Value0Of4<T0, T1, T2, T3>
  implements
  MapFunction<Tuple4<T0, T1, T2, T3>, T0>,
  KeySelector<Tuple4<T0, T1, T2, T3>, T0> {

  @Override
  public T0 map(Tuple4<T0, T1, T2, T3> quadruple) throws Exception {
    return quadruple.f0;
  }

  @Override
  public T0 getKey(Tuple4<T0, T1, T2, T3> quadruple) throws Exception {
    return quadruple.f0;
  }
}
