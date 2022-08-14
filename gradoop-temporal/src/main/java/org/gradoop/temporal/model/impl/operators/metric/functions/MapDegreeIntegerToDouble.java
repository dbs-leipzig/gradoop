/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * A map function which converts the degree in a Tuple from Integer to Double.
 */
public class MapDegreeIntegerToDouble implements MapFunction<Tuple1<Integer>, Tuple1<Double>> {

  @Override
  public Tuple1<Double> map(Tuple1<Integer> degree) throws Exception {
    int intDegree = degree.f0;
    Double doubleDegree = (double) intDegree;
    return new Tuple1<>(doubleDegree);
  }
}
