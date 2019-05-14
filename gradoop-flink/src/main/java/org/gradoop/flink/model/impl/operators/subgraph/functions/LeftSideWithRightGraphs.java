/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMGraphElement;

/**
 * left, right => left (retain graphIds contained in right)
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.NonForwardedFieldsFirst("graphIds")
@FunctionAnnotation.ReadFieldsSecond("graphIds")
public class LeftSideWithRightGraphs<L extends EPGMGraphElement, R extends EPGMGraphElement>
  implements JoinFunction<L, R, L> {
  @Override
  public L join(L left, R right) throws Exception {
    left.getGraphIds().retainAll(right.getGraphIds());
    return left;
  }
}
