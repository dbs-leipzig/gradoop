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
package org.gradoop.flink.io.impl.tlf;

/**
 * Constants needed for Dictionary functions
 */
public class TLFConstants {

  /**
   * String used to identify the vertex dictionary on broadcast.
   */
  public static final String VERTEX_DICTIONARY = "vertexDictionary";

  /**
   * String used to identify the edge dictionary on broadcast.
   */
  public static final String EDGE_DICTIONARY = "edgeDictionary";
  /**
   * Constant string which is added to those edges or vertices which do not
   * have an entry in the dictionary while others have one.
   */
  public static final String EMPTY_LABEL = "";
  /**
   * TLF graph number indicator
   */
  public static final String NEW_GRAPH_TAG = "#";

  // record reader constants

  /**
   * Start tag of a tlf graph.
   */
  public static final String START_TAG = "t #";

  /**
   * End tag of a tlf graph, which is not only the end tag but also the start tag of the next graph.
   */
  public static final String END_TAG = "t #";

  // tuple constants

  /**
   * Symbol identifying a line to represent an edge.
   */
  public static final String EDGE_SYMBOL = "e";
  /**
   * Symbol identifying a line to represent a graph start.
   */
  public static final String GRAPH_SYMBOL = "t";
  /**
   * Symbol identifying a line to represent a vertex.
   */
  public static final String VERTEX_SYMBOL = "v";

}
