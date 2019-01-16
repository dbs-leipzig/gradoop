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
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.api.epgm.BaseGraph;

/**
 * Creates a value of type {@link V} from one input graph of type {@link G}.
 *
 * @param <G> the input graph type
 * @param <V> the type of the return value
 */
public interface UnaryBaseGraphToValueOperator<G extends BaseGraph, V> extends Operator {
  /**
   * Executes the operator on the given graph.
   *
   * @param graph input graph
   * @return operator result
   */
  V execute(G graph);
}
