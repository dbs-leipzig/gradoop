/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
