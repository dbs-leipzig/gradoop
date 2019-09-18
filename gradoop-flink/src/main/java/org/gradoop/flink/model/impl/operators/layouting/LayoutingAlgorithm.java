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
package org.gradoop.flink.model.impl.operators.layouting;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Base-class for all Layouters
 */
public interface LayoutingAlgorithm extends UnaryGraphToGraphOperator {

  /**
   * The name of the property where the X-Coordinate of a vertex is stored
   */
  String X_COORDINATE_PROPERTY = "X";


  /**
   * The name of the property where the Y-Coordinate of a vertex is stored
   */
  String Y_COORDINATE_PROPERTY = "Y";

  /**
   * The name of the property where the layout-width is stored in the graph-head
   */
  String LAYOUT_WIDTH = "WIDTH";

  /**
   * The name of the property where the layout-height is stored in the graph-head
   */
  String LAYOUT_HEIGHT = "HEIGHT";

  /**
   * Layouts the given graph. After layouting all vertices will have two new properties:
   * X: the assigned x-coordinate
   * Y: the assigned y-coordinate
   *
   * @param g The graph to layout
   * @return The input-graph, but every vertex now has X and Y coordinates as properties
   */
  LogicalGraph execute(LogicalGraph g);

  /**
   * The width of the layout-area for this layouter
   *
   * @return The width
   */
  int getWidth();

  /**
   * The height of the layout-area for this layouter
   *
   * @return The height
   */
  int getHeight();
}
