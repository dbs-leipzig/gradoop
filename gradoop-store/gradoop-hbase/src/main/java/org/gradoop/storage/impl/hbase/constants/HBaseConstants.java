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
package org.gradoop.storage.impl.hbase.constants;

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
   * Column family name for property type.
   */
  public static final String CF_PROPERTY_TYPE = "p_type";
  /**
   * Column family name for property value.
   */
  public static final String CF_PROPERTY_VALUE = "p_value";
  /**
   * Column identifier for source vertex identifier.
   */
  public static final String COL_SOURCE = "s";
  /**
   * Column identifier for target vertex identifier.
   */
  public static final String COL_TARGET = "t";
}
