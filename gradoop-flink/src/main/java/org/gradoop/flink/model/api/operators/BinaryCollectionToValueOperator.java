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
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.epgm.GraphCollection;
/**
 * Creates a (usually 1-element) Boolean dataset based on two input graphs.
 *
 * @param <T> value type
 */
public interface BinaryCollectionToValueOperator<T> extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstCollection  first input collection
   * @param secondCollection second input collection
   * @return operator result
   */
  DataSet<T> execute(GraphCollection firstCollection,
    GraphCollection secondCollection);
}
