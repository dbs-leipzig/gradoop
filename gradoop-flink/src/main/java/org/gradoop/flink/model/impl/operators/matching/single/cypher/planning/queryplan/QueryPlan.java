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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Represents an executable Cypher query plan.
 */
public class QueryPlan {
  /**
   * Used for indentation when creating a string representation of the plan
   */
  private static final String PAD_STRING = "|.";
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
   * @return root node of the query plan
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    printPlanNode(root, 0, sb);
    return sb.toString();
  }

  /**
   * Recursively prints the sub tree of the given node in pre-order.
   *
   * @param node root plan node
   * @param level level of the whole query tree
   * @param sb string builder to append
   */
  private void printPlanNode(PlanNode node, int level, StringBuilder sb) {
    sb.append(String.format("%s|-%s%n", StringUtils.leftPad("", level * 2, PAD_STRING), node));
    level++;
    if (node instanceof UnaryNode) {
      printPlanNode(((UnaryNode) node).getChildNode(), level, sb);
    } else if (node instanceof BinaryNode) {
      printPlanNode(((BinaryNode) node).getLeftChild(), level, sb);
      printPlanNode(((BinaryNode) node).getRightChild(), level, sb);
    }
  }
}
