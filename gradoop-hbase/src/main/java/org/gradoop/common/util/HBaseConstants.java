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
package org.gradoop.common.util;

/**
 * Constants used in Gradoop.
 */
public final class HBaseConstants {
  /**
   * Default HBase table name for graph heads.
   */
  public static final String DEFAULT_TABLE_GRAPHS = "graph_heads";
  /**
   * Default HBase table name for vertices.
   */
  public static final String DEFAULT_TABLE_VERTICES = "vertices";
  /**
   * Default HBase table name for edges.
   */
  public static final String DEFAULT_TABLE_EDGES = "edges";

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
   * String representation of {@code null}.
   */
  public static final String NULL_STRING = "NULL";

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
  /**
   * Default label of an EPGM database graph.
   */
  public static final String DB_GRAPH_LABEL = "_DB";
}
