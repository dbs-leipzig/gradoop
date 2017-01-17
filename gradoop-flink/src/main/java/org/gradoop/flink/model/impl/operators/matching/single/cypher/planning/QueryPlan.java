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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

/**
 * Represents an executable Cypher query plan.
 */
public class QueryPlan {
  /**
   * Root entry point for the query plan.
   */
  private final PlanNode root;

  /**
   * Creates a new query plan
   *
   * @param root root node of the query plan
   */
  public QueryPlan(PlanNode root) {
    this.root = root;
  }

  /**
   * Returns the root node of the query plan
   *
   * @return
   */
  public PlanNode getRoot() {
    return root;
  }

  /**
   * Executes the query plan and produces the resulting embeddings.
   *
   * @return embeddings representing the query result
   */
  public DataSet<Embedding> execute() {
    return root.execute();
  }
}
