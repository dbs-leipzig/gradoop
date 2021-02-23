/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

import java.io.Serializable;

/**
 * A function that can calculate the similarity between two vertices.
 */
public interface VertexCompareFunction extends Serializable {
  /**
   * Computes a numerical similarity between two vertices
   *
   * @param v1 Vertex 1
   * @param v2 Vertex 2
   * @return a number between 0 and 1 (inclusive). 0 meaning completely different and 1 meaning
   * identical.
   */
  double compare(LVertex v1, LVertex v2);
}
