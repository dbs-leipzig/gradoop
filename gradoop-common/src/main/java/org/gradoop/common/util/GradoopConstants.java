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
package org.gradoop.common.util;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Constants used in Gradoop.
 */
public final class GradoopConstants {
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
   * Static identifier for the database graph.
   */
  public static final GradoopId DB_GRAPH_ID = GradoopId.fromString("598349bcda43031d1ea62d3b");
  /**
   * Default label of an database graph.
   */
  public static final String DB_GRAPH_LABEL = "_DB";
}
