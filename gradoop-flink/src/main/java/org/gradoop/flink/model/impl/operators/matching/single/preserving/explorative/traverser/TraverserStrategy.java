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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser;

/**
 * Defines the strategy to traverse the graph.
 */
public enum TraverserStrategy {
  /**
   * Traverse the graph using bulk iteration.
   */
  SET_PAIR_BULK_ITERATION,
  /**
   * Traverse the graph using a for loop iteration.
   */
  SET_PAIR_FOR_LOOP_ITERATION,
  /**
   * Traverse the graph based on edge triples in a for loop.
   */
  TRIPLES_FOR_LOOP_ITERATION
}
