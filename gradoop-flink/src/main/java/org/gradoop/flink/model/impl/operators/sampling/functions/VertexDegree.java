/**
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

import java.security.InvalidParameterException;

/**
 * Keeps the types of vertex-degrees and the respective property names.
 */
public class VertexDegree {

  /**
   * Types of vertex degrees
   */
  public enum DegreeType {
    /**
     * Input degree
     */
    InputDegree,
    /**
     * Output degree
     */
    OutputDegree,
    /**
     * Sum of both
     */
    Degree
  }

  /**
   * In-degree property name
   */
  public static final String IN_DEGREE_PROPERTY_NAME = "_inDegree";

  /**
   * Out-degree property name
   */
  public static final String OUT_DEGREE_PROPERTY_NAME = "_outDegree";

  /**
   * Degree property name
   */
  public static final String DEGREE_PROPERTY_NAME = "_degree";

  /**
   * Build an instance of DegreeType from a given string
   * @param degreeType The vertex degree type, e.g. "InputDegree"
   * @return An instance for DegreeType
   */
  public static DegreeType fromString(String degreeType) {
    if (degreeType.equals(DegreeType.InputDegree.toString())) {
      return DegreeType.InputDegree;
    } else if (degreeType.equals(DegreeType.OutputDegree.toString())) {
      return DegreeType.OutputDegree;
    } else if (degreeType.equals(DegreeType.Degree.toString())) {
      return DegreeType.Degree;
    } else {
      throw new InvalidParameterException();
    }
  }
}
