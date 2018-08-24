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
package org.gradoop.flink.model.impl.operators.sampling.functions;

/**
 * Keeps the types of vertex-neighborhood regarding the connecting edges
 * and their respective property names.
 * Available neighborhood-types: input (IN), output (OUT), both (BOTH).
 */
public enum Neighborhood {
  /**
   * Input edges
   */
  IN("_InNeighbor"),
  /**
   * Output edges
   */
  OUT("_OutNeighbor"),
  /**
   * Both edges
   */
  BOTH("_InOutNeighbor");

  /**
   * The property name of a vertex neighbor type
   */
  private String neighborPropertyName;

  /**
   * Enum constructor for neighbor type with property name.
   *
   * @param neighborPropertyName The property name for a neighbor type
   */
  Neighborhood(String neighborPropertyName) {
    this.neighborPropertyName = neighborPropertyName;
  }

  /**
   * Get the property name for an instance of Neighborhood.
   *
   * @return The neighbor property name
   */
  public String getName() {
    return neighborPropertyName;
  }
}
