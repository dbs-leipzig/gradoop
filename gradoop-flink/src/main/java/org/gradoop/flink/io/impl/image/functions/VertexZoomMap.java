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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.impl.image.ImageDataSink;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

import java.util.List;

/**
 * Function to align vertices to increased image size.
 */
public class VertexZoomMap extends RichMapFunction<EPGMVertex, EPGMVertex> {

  /**
   * X-Coordinate Offset
   */
  private int offsetX = 0;
  /**
   * Y-Coordinate Offset
   */
  private int offsetY = 0;
  /**
   * Default zoomFactor
   */
  private double zoomFactor = 1;
  /**
   * Width of output-image (px)
   */
  private final int imageWidth;
  /**
   * Height of output-image (px)
   */
  private final int imageHeight;
  /**
   * Additional border for alignment
   */
  private final int zoomBorder;

  /**
   * Constructor
   *
   * @param imageWidth    given image width
   * @param imageHeightF  given image height
   * @param zoomBorder    given additional border after zooming
   */
  public VertexZoomMap(int imageWidth, int imageHeightF, int zoomBorder) {
    this.imageWidth = imageWidth;
    this.imageHeight = imageHeightF;
    this.zoomBorder = zoomBorder;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Tuple4<Integer, Integer, Integer, Integer>> minmaxlist = getRuntimeContext()
      .getBroadcastVariable(ImageDataSink.BORDER_BROADCAST);
    offsetX = minmaxlist.get(0).f0;
    offsetY = minmaxlist.get(0).f1;
    int maxX = minmaxlist.get(0).f2;
    int maxY = minmaxlist.get(0).f3;
    int xRange = maxX - offsetX;
    int yRange = maxY - offsetY;
    zoomFactor = (xRange > yRange) ? imageWidth / (double) xRange : imageHeight / (double) yRange;
  }

  @Override
  public EPGMVertex map(EPGMVertex v) {
    int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
    int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
    x = (int) ((x - offsetX) * zoomFactor) + zoomBorder;
    y = (int) ((y - offsetY) * zoomFactor) + zoomBorder;
    v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, x);
    v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, y);
    return v;
  }
}
