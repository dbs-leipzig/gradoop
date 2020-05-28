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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

/**
 * Function to align vertices to image without additional zooming.
 */
public class VertexScaleMap implements MapFunction<EPGMVertex, EPGMVertex> {

  /**
   * Width scale
   */
  private double widthScale;
  /**
   * Height scale
   */
  private double heightScale;

  /**
   * Constructor
   *
   * @param widthScale    width scale
   * @param heightScale   height scale
   */
  public VertexScaleMap(double widthScale, double heightScale) {
    this.widthScale = widthScale;
    this.heightScale = heightScale;
  }

  @Override
  public EPGMVertex map(EPGMVertex v) throws Exception {
    int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
    int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
    x = (int) (x * widthScale);
    y = (int) (y * heightScale);
    v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, x);
    v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, y);
    return v;
  }
}
