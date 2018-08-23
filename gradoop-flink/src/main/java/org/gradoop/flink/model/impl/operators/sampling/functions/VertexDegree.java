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
 * Keeps the types of vertex-degrees and their respective property names.
 * Available degree-types: input (IN), output (OUT), sum of both (BOTH).
 */
public enum VertexDegree {
  /**
   * The input degree
   */
  IN("_InDegree"),
  /**
   * The output degree
   */
  OUT("_OutDegree"),
  /**
   * The sum of input and output degree
   */
  BOTH("_Degree");

  /**
   * The property name of a vertex degree type
   */
  private String degreePropertyName;

  /**
   * Enum constructor for degree type with property name.
   * @param degreePropertyName The property name for a degree type
   */
  VertexDegree(String degreePropertyName) {
    this.degreePropertyName = degreePropertyName;
  }

  /**
   * Get the property name for an instance of VertexDegree.
   * @return The degree property name
   */
  public String getName() {
    return degreePropertyName;
  }
}
