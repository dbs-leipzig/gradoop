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

import java.security.InvalidParameterException;

/**
 * Keeps the types of neighborhood
 */
public class Neighborhood {
  /**
   * Types of neighborhood: input, output, or both edges
   */
  public enum NeighborType {
    /**
     * Input edges
     */
    Input,
    /**
     * Output edges
     */
    Output,
    /**
     * Both edges
     */
    Both
  }

  /**
   * Build an instance of NieghborType from a given string
   * @param neighborType type of neighborhood
   * @return an instace of NieghborType
   */
  public static NeighborType fromString(String neighborType) {
    if (neighborType.equals(NeighborType.Input.toString())) {
      return NeighborType.Input;
    } else if (neighborType.equals(NeighborType.Output.toString())) {
      return NeighborType.Output;
    } else if (neighborType.equals(NeighborType.Both.toString())) {
      return NeighborType.Both;
    } else {
      throw new InvalidParameterException();
    }
  }
}
