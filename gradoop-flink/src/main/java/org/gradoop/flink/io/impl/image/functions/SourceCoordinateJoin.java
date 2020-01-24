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
package org.gradoop.flink.io.impl.image.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.impl.image.ImageDataSink;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

/**
 * Adds source X and Y coordinates to the corresponding edge
 */
public class SourceCoordinateJoin implements JoinFunction<EPGMEdge, EPGMVertex, EPGMEdge> {

  /**
   * Default Constructor
   */
  public SourceCoordinateJoin() {

  }

  @Override
  public EPGMEdge join(EPGMEdge first, EPGMVertex second) throws Exception {
    first
      .setProperty(ImageDataSink.SOURCE_X, second.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY));
    first
      .setProperty(ImageDataSink.SOURCE_Y, second.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY));
    return first;
  }
}
