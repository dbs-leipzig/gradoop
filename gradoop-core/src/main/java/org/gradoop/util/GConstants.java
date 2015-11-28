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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.util;

/**
 * Constants used in Gradoop.
 */
public final class GConstants {
  /**
   * Default HBase table name for vertex data.
   */
  public static final String DEFAULT_TABLE_VERTICES = "vertex_data";
  /**
   * Default HBase table name for edge data.
   */
  public static final String DEFAULT_TABLE_EDGES = "edge_data";
  /**
   * Default HBase table name for graph data.
   */
  public static final String DEFAULT_TABLE_GRAPHS = "graph_data";

  /**
   * Default label for unlabeled vertices.
   */
  public static final String DEFAULT_VERTEX_LABEL = "";
  /**
   * Default label for unlabeled graphs.
   */
  public static final String DEFAULT_GRAPH_LABEL = "";
  /**
   * Default label for unlabeled edges.
   */
  public static final String DEFAULT_EDGE_LABEL = "";

  /**
   * Column family name for label.
   */
  public static final String CF_META = "m";
  /**
   * Column identifier for label.
   */
  public static final String COL_LABEL = "l";
  /**
   * Column identifier for graphs.
   */
  public static final String COL_GRAPHS = "g";
  /**
   * Column family name for properties.
   */
  public static final String CF_PROPERTIES = "p";
  /**
   * Column family name for vertices.
   */
  public static final String CF_VERTICES = "v";
  /**
   * Column family for edges.
   */
  public static final String CF_EDGES = "e";
  /**
   * Column family name for outgoing edges.
   */
  public static final String CF_OUT_EDGES = "oe";
  /**
   * Column family name for incoming edges.
   */
  public static final String CF_IN_EDGES = "ie";
  /**
   * Column identifier for source vertex identifier.
   */
  public static final String COL_SOURCE = "s";
  /**
   * Column identifier for target vertex identifier.s
   */
  public static final String COL_TARGET = "t";

  /**
   * Default cache size for scans in HBase.
   */
  public static final int HBASE_DEFAULT_SCAN_CACHE_SIZE = 500;
}
