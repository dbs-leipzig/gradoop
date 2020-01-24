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

import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * A layouter that combines the {@link CentroidFRLayouter } and the {@link FRLayouter}.
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
  private CentroidFRLayouter centroidFRLayouter;
  /**
   * The FRLayouter that is being used
   */
  private FRLayouter fRLayouter;

  /**
   * Create a new CombiLayouter.
   *
   * @param iterations  Number of iterations to perform
   * @param numVertices Approximate number of vertices in the input-graph
   * @param quality     Ratio of iterations between the two base algorithms. The higher the value,
   *                    the more iterations of the FRLayouter are performed. A value of 0.1 is often
   *                    good enough.
   */
  public CombiLayouter(int iterations, int numVertices, double quality) {

    this.iterations = iterations;
    this.numberOfVertices = numVertices;
    this.quality = quality;
    int centroidIterations = (int) Math.floor(iterations * (1 - quality));
    int frIterations = (int) Math.ceil(iterations * quality);

    if (centroidIterations > 0) {
      centroidFRLayouter = new CentroidFRLayouter(centroidIterations, numVertices);
      centroidFRLayouter.k(centroidFRLayouter.getK() * K_FACTOR);
    }
    if (frIterations > 0) {
      fRLayouter =
        new FRLayouter(frIterations, numVertices).useExistingLayout(centroidFRLayouter != null)
          .startAtIteration(centroidIterations);
    }
  }

  /**
   * Set the FRLayouter parameter k for both algorithms, overwriting the default value
   *
   * @param k The new value
   * @return this layouter
   */
  public CombiLayouter k(double k) {
    if (centroidFRLayouter != null) {
      centroidFRLayouter.k(k * K_FACTOR);
    }
    if (fRLayouter != null) {
      fRLayouter.k(k);
    }
    return this;
  }

  /**
   * Override default layout-space size
   * Default:  {@code width = height = Math.sqrt(Math.pow(k, 2) * numberOfVertices) * 0.5}
   *
   * @param width  new width
   * @param height new height
   * @return this layouter
   */
  public CombiLayouter area(int width, int height) {
    if (centroidFRLayouter != null) {
      centroidFRLayouter.area(width, height);
      if (fRLayouter != null) {
        fRLayouter.area(width, height);
      }
    }
    return this;
  }

  /**
   * Override default maxRepulsionDistance of the FR-Algorithm. Vertices with larger distance
   * are ignored in repulsion-force calculation
   * Default-Value is relative to current {@code k}. If {@code k} is overriden, this is changed
   * accordingly automatically
   * Default: 2k
   *
   * @param maxRepulsionDistance new value
   * @return this layouter
   */
  public CombiLayouter maxRepulsionDistance(int maxRepulsionDistance) {
    if (fRLayouter != null) {
      fRLayouter.maxRepulsionDistance(maxRepulsionDistance);
    }
    return this;
  }

  /**
   * Use the existing layout as starting point instead of creating a random one.
   * If used, EVERY vertex in the input-graph MUST have an X and Y property!
   *
   * @param useExisting whether to re-use the existing layout or not
   * @return this layouter
   */
  public CombiLayouter useExistingLayout(boolean useExisting) {
    if (centroidFRLayouter != null) {
      centroidFRLayouter.useExistingLayout(useExisting);
    } else {
      fRLayouter.useExistingLayout(useExisting);
    }
    return this;
  }

  /**
   * Gets k. K is the distance in which the attracting and repulsive forces between two connected
   * vertices are equally strong. The value of k influences the scale of the final graph-layout.
   * (A small k leads to vertices being closer together)
   *
   * @return value of k
   */
  public double getK() {
    if (fRLayouter != null) {
      return fRLayouter.getK();
    } else {
      return centroidFRLayouter.getK() / K_FACTOR;
    }
  }

  /**
   * Gets maxRepulsionDistance
   *
   * @return value of maxRepulsionDistance
   */
  public int getMaxRepulsionDistance() {
    if (fRLayouter == null) {
      return -1;
    }
    return fRLayouter.getMaxRepulsionDistance();
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {
    if (centroidFRLayouter != null) {
      g = centroidFRLayouter.execute(g);
    }
    if (fRLayouter != null) {
      g = fRLayouter.execute(g);
    }
    return g;
  }

  @Override
  public int getWidth() {
    if (fRLayouter != null) {
      return fRLayouter.getWidth();
    } else {
      return centroidFRLayouter.getWidth();
    }
  }

  @Override
  public int getHeight() {
    if (fRLayouter != null) {
      return fRLayouter.getHeight();
    } else {
      return centroidFRLayouter.getHeight();
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
