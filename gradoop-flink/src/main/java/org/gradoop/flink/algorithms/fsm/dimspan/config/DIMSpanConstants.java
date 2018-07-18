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
package org.gradoop.flink.algorithms.fsm.dimspan.config;

/**
 * Collection of broadcast dataset names used for frequent subgraph mining.
 */
public class DIMSpanConstants {

  /**
   * Cardinality of input graph collection size.
   */
  public static final String GRAPH_COUNT = "|G|";

  /**
   * Minimum frequency derived from min support und graph count.
   */
  public static final String MIN_FREQUENCY = "fmin";

  /**
   * Vertex label dictionary.
   */
  public static final String VERTEX_DICTIONARY = "vld";

  /**
   * Edge label Dictionary
   */
  public static final String EDGE_DICTIONARY = "eld";

  /**
   * set of frequent patterns
   */
  public static final String FREQUENT_PATTERNS = "fp";

  /**
   * Graph head label of frequent patterns
   */
  public static final String FREQUENT_PATTERN_LABEL = "FrequentPattern";

  /**
   * Property key to store a frequent subgraphs's support.
   */
  public static final String SUPPORT_KEY = "support";

  /**
   * Property key to store the canonical label.
   */
  public static final String CANONICAL_LABEL_KEY = "minDFS";
}
