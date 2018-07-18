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
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.epgm.GraphCollection;

/**
 * Creates a value from one input collection.
 *
 * @param <T> result type
 */
public interface UnaryGraphCollectionToValueOperator<T> {
  /**
   * Executes the operator.
   *
   * @param collection input collection
   * @return operator result
   */
  DataSet<T> execute(GraphCollection collection);
}
