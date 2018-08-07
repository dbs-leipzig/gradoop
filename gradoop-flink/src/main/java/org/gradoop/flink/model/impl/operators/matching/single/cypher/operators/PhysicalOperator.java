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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Physical Operators are used to transform input data into Embeddings
 * Chaining physical operators will execute a query
 */
public interface PhysicalOperator {

  /**
   * Runs the operator on the input data
   * @return The resulting embedding
   */
  DataSet<Embedding> evaluate();

  /**
   * Set the operator description
   * This is used for Flink operator naming
   * @param newName operator description
   */
  void setName(String newName);

  /**
   * Get the operator description
   * This is used for Flink operator naming
   * @return operator description
   */
  String getName();

}
