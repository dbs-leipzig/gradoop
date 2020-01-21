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
package org.gradoop.flink.model.impl.operators.layouting;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Interface for all Layouters. A LayoutAlgorithm layouts a graph (assignes a position in 2D-space
 * to each vertex in a graph). Layouted graphs can be converted to
 * images using {@link org.gradoop.flink.io.impl.image.ImageDataSink}.
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
   * Layouts the given graph. After layouting all vertices will have two new properties:
   * X: the assigned x-coordinate <br>
   * Y: the assigned y-coordinate <br>
   *
   * @param inputGraph The graph to layout
   * @return The input-graph, but every vertex now has X and Y coordinates as properties
   */
  LogicalGraph execute(LogicalGraph inputGraph);

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
