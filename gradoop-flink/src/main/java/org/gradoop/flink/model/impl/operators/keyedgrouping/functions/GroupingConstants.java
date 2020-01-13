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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

/**
 * Holds common constants used by functions in keyed grouping.
 */
public abstract class GroupingConstants {
  /**
   * The index of the vertex ID in the tuple-representation of a vertex.
   */
  public static final int VERTEX_TUPLE_ID = 0;
  /**
   * The index of the super vertex ID in the tuple-representation of a vertex.
   */
  public static final int VERTEX_TUPLE_SUPERID = 1;
  /**
   * The number of reserved fields in the tuple-representation of a vertex.
   */
  public static final int VERTEX_TUPLE_RESERVED = 2;
  /**
   * The index of the source ID in the tuple-representation of an edge.
   */
  public static final int EDGE_TUPLE_SOURCEID = 0;
  /**
   * The index of the target ID in the tuple-representation of an edge.
   */
  public static final int EDGE_TUPLE_TARGETID = 1;
  /**
   * The number of reserved fields in the tuple-representation of an edge.
   */
  public static final int EDGE_TUPLE_RESERVED = 2;
}
