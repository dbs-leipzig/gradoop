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

import java.util.Objects;

/**
 * A unary node takes a single data set containing embeddings as input which is used to compute
 * a new data set of embeddings based on the specific node implementation.
 */
public abstract class UnaryNode extends PlanNode {
  /**
   * Represents the input of that node.
   */
  private final PlanNode childNode;

  /**
   * Creates a new unary node
   *
   * @param childNode input of the node
   */
  public UnaryNode(PlanNode childNode) {
    Objects.requireNonNull(childNode);
    this.childNode = childNode;
  }

  /**
   * Returns the child (input) node of that node.
   *
   * @return child plan node
   */
  public PlanNode getChildNode() {
    return childNode;
  }
}
