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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => graphHead
 *
 * @param <T> tuple type
 */
@FunctionAnnotation.ForwardedFields("f0->*")
@FunctionAnnotation.ReadFields("f0")
public class TransactionGraphHead<T extends Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> implements
  MapFunction<T, GraphHead> {

  @Override
  public GraphHead map(T triple) throws Exception {
    return triple.f0;
  }
}
