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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.concurrent.ThreadLocalRandom;

/**
 * LayoutingAlgorithm that positions all vertices randomly
 */
public class RandomLayouter implements LayoutingAlgorithm, MapFunction<EPGMVertex, EPGMVertex> {

  /**
   * Minimum value for x coordinates
   */
  private int minX;
  /**
   * Maximum value for x coordinates
   */
  private int maxX;
  /**
   * Minimum value for y coordinates
   */
  private int minY;
  /**
   * Maximum value for y coordinates
   */
  private int maxY;
  /**
   * Rng to use for coordinate-generation
   */
  private ThreadLocalRandom rng;

  /**
   * Create a new RandomLayouter
   *
   * @param minX Minimum value of x-coordinate
   * @param maxX Maximum value of x-coordinate
   * @param minY Minimum value of y-coordinate
   * @param maxY Maximum value of y-coordinate
   */
  public RandomLayouter(int minX, int maxX, int minY, int maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {
    if (rng == null) {
      rng = ThreadLocalRandom.current();
    }
    DataSet<EPGMVertex> placed = g.getVertices().map(this);
    return g.getFactory().fromDataSets(placed, g.getEdges());
  }

  @Override
  public EPGMVertex map(EPGMVertex vertex) throws Exception {
    vertex.setProperty(X_COORDINATE_PROPERTY, rng.nextInt(maxX - minX) + minX);
    vertex.setProperty(Y_COORDINATE_PROPERTY, rng.nextInt(maxY - minY) + minY);
    return vertex;
  }

  @Override
  public int getWidth() {
    return maxX;
  }

  @Override
  public int getHeight() {
    return maxY;
  }

  @Override
  public String toString() {
    return "RandomLayouter{" + "minX=" + minX + ", maxX=" + maxX + ", minY=" + minY + ", maxY=" +
      maxY + ", rng=" + rng + '}';
  }
}
