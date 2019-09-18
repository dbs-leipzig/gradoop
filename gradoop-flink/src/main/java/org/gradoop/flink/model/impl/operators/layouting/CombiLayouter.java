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

import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * A layouter that combines the CentroidFRLayouter and the FRLayouter
 */
public class CombiLayouter implements LayoutingAlgorithm {
  /**
   * Factor by which the k of the CentroidFRLayouter is scaled
   */
  private static final double K_FACTOR = 1.3;
  /**
   * Number of iterations to perform
   */
  private int iterations;
  /**
   * Approximate number of vertices in the input-graph
   */
  private int numberOfVertices;
  /**
   * The ratio of iterations of the CentroidFRLayouter and the FRLayouter
   */
  private double quality;
  /**
   * The CentroidFRLayouter that is being used
   */
  private CentroidFRLayouter layouter1;
  /**
   * The FRLayouter that is being used
   */
  private FRLayouter layouter2;

  /**
   * Create a new CombiLayouter
   * @param iterations Number of iterations to perform
   * @param numVertices Approximate number of vertices in the input-graph
   * @param quality Ratio of iterations between the two base algorithms. The higher the value,
   *                the more iterations of the FRLayouter are performed. A value of 0.1 is often
   *                good enough.
   */
  public CombiLayouter(int iterations, int numVertices, double quality) {

    this.iterations = iterations;
    this.numberOfVertices = numVertices;
    this.quality = quality;
    int l1iterations = (int) Math.floor(iterations * (1 - quality));
    int l2iterations = (int) Math.ceil(iterations * quality);

    if (l1iterations > 0) {
      layouter1 = new CentroidFRLayouter(l1iterations, numVertices);
      layouter1.k(layouter1.getK() * K_FACTOR);
    }
    if (l2iterations > 0) {
      layouter2 = new FRLayouter(l2iterations, numVertices).useExistingLayout(layouter1 != null)
        .startAtIteration(l1iterations);
    }
  }

  /**
   * Set the FRLayouter parameter k for both algorithms, overwriting the default value
   * @param k The new value
   * @return this (for method chaining)
   */
  public CombiLayouter k(double k) {
    if (layouter1 != null) {
      layouter1.k(k * K_FACTOR);
    }
    if (layouter2 != null) {
      layouter2.k(k);
    }
    return this;
  }

  /**
   * Override default layout-space size
   * Default:  width = height = Math.sqrt(Math.pow(k, 2) * numberOfVertices) * 0.5
   *
   * @param width  new width
   * @param height new height
   * @return this (for method-chaining)
   */
  public CombiLayouter area(int width, int height) {
    if (layouter1 != null) {
      layouter1.area(width, height);
      if (layouter2 != null) {
        layouter2.area(width, height);
      }
    }
    return this;
  }

  /**
   * Override default maxRepulsionDistance of the FR-Algorithm. Vertices with larger distance
   * are ignored in repulsion-force calculation
   * Default-Value is relative to current k. If k is overriden, this is changed
   * accordingly automatically
   * Default: 2k
   *
   * @param maxRepulsionDistance new value
   * @return this (for method-chaining)
   */
  public CombiLayouter maxRepulsionDistance(int maxRepulsionDistance) {
    if (layouter2 != null) {
      layouter2.maxRepulsionDistance(maxRepulsionDistance);
    }
    return this;
  }

  /**
   * Use the existing layout as starting point instead of creating a random one.
   * If used, EVERY vertex in the input-graph MUST have an X and Y property!
   * @param uel whether to re-use the existing layout or not
   * @return this (for method chaining)
   */
  public CombiLayouter useExistingLayout(boolean uel) {
    if (layouter1 != null) {
      layouter1.useExistingLayout(uel);
    } else {
      layouter2.useExistingLayout(uel);
    }
    return this;
  }

  /**
   * Gets k
   *
   * @return value of k
   */
  public double getK() {
    if (layouter2 != null) {
      return layouter2.getK();
    } else {
      return layouter1.getK() / K_FACTOR;
    }
  }

  /**
   * Gets maxRepulsionDistance
   *
   * @return value of maxRepulsionDistance
   */
  public int getMaxRepulsionDistance() {
    if (layouter2 == null) {
      return -1;
    }
    return layouter2.getMaxRepulsionDistance();
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {
    if (layouter1 != null) {
      g = layouter1.execute(g);
    }
    if (layouter2 != null) {
      g = layouter2.execute(g);
    }
    return g;
  }

  @Override
  public int getWidth() {
    if (layouter2 != null) {
      return layouter2.getWidth();
    } else {
      return layouter1.getWidth();
    }
  }

  @Override
  public int getHeight() {
    if (layouter2 != null) {
      return layouter2.getHeight();
    } else {
      return layouter1.getHeight();
    }
  }

  @Override
  public String toString() {
    return "CombiFRLayouter{" + " quality=" + quality + ", iterations=" + iterations + ", k=" +
      getK() + "," + " " + "with=" + getWidth() + ", height=" + getHeight() +
      ", maxRepulsionDistance=" + getMaxRepulsionDistance() + ", numberOfVertices=" +
      numberOfVertices + '}';
  }
}
