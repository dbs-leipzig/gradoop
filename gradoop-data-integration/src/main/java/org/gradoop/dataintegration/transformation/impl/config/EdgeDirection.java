/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.transformation.impl.config;

/**
 * This ENUM represents possible edge directions for newly created edges.
 */
public enum EdgeDirection {
  /**
   * No edge is created.
   */
  NONE,

  /**
   * The edge points from the original vertex to the new one.
   */
  ORIGIN_TO_NEWVERTEX,

  /**
   * The edge points from the newly created vertex to the original vertex.
   */
  NEWVERTEX_TO_ORIGIN,

  /**
   * Two edges are created between the orignal and the new vertex.
   */
  BIDIRECTIONAL
}
