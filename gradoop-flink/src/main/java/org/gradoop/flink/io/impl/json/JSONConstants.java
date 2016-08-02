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

package org.gradoop.flink.io.impl.json;

/**
 * Constants needed for the JSON IO classes.
 */
public class JSONConstants {

  /**
   * Key for vertex, edge and graph id.
   */
  public static final String IDENTIFIER = "id";
  /**
   * Key for meta Json object.
   */
  public static final String META = "meta";
  /**
   * Key for data Json object.
   */
  public static final String DATA = "data";
  /**
   * Key for vertex, edge and graph label.
   */
  public static final String LABEL = "label";
  /**
   * Key for graph identifiers at vertices and edges.
   */
  public static final String GRAPHS = "graphs";
  /**
   * Key for vertex identifiers at graphs.
   */
  public static final String VERTICES = "vertices";
  /**
   * Key for edge identifiers at graphs.
   */
  public static final String EDGES = "edges";
  /**
   * Key for edge source vertex id.
   */
  public static final String EDGE_SOURCE = "source";
  /**
   * Key for edge target vertex id.
   */
  public static final String EDGE_TARGET = "target";
}
