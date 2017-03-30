/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
