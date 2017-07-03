
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
