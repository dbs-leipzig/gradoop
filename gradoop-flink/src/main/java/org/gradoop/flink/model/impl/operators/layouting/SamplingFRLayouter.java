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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdSelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

import java.util.concurrent.ThreadLocalRandom;

/**
 * FRLayouter that only considers a random sample of vertices for one side of the
 * repulsion-calculation. Depending on the sampleRate this algorithm is faster then the default
 * one but produces only approximate images.
 */
public class SamplingFRLayouter extends FRLayouter {

  /**
   * probability to choose a given vertex for repulsion calculations. {@code >0 <1 }
   */
  protected double samplingRate;

  /**
   * Create new Instance of FRLayouter.
   *
   * @param iterations   Number of iterations to perform
   * @param vertexCount  (Estimated) number of vertices in the graph. Needed to calculate default
   * @param samplingRate Factor {@code >0 <=1 } to simplify repulsion-computation. Lower values are
   *                     faster but less precise
   */
  public SamplingFRLayouter(int iterations, int vertexCount, double samplingRate) {
    super(iterations, vertexCount);

    if (samplingRate <= 0 || samplingRate > 1) {
      throw new IllegalArgumentException("Sampling rate must be greater than 0 and less than or " +
        "equal to 1.");
    }
    this.samplingRate = samplingRate;
  }

  @Override
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    vertices = vertices.map(new FRCellIdMapper(getMaxRepulsionDistance()));

    KeySelector<LVertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRRepulsionFunction repulsionFunction =
      new FRRepulsionFunction(getK(), getMaxRepulsionDistance());

    final double samplingRateF = this.samplingRate;
    DataSet<LVertex> sampledVertices = vertices.filter(
      (FilterFunction<LVertex>) lVertex -> ThreadLocalRandom.current().nextDouble(1) < samplingRateF);

    DataSet<Force> self = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> up = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UP)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> down = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWN)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> left = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> right = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.RIGHT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> uright = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> dright = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNRIGHT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> uleft = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> dleft = vertices
      .join(sampledVertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNLEFT)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);


    return
      self.union(up).union(left).union(uright).union(uleft).union(down).union(right).union(dright)
        .union(dleft)
        .map(f -> {
          f.getValue().mDiv(samplingRateF);
          return f;
        });
  }

  @Override
  public String toString() {
    return "SamplingFRLayouter{" + "samplingRate=" + samplingRate + ", iterations=" + iterations +
      ", k=" + getK() + ", width=" + getWidth() + ", height=" + getHeight() +
      ", maxRepulsionDistance=" + getMaxRepulsionDistance() + ", numberOfVertices=" +
      numberOfVertices + ", useExistingLayout=" + useExistingLayout + '}';
  }
}
