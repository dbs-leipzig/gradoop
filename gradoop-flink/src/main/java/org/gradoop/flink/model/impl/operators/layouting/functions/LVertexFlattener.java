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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Converts an LVertex (that can contain multiple sub-vertices into a list of single LVertices.
 */
public class LVertexFlattener implements FlatMapFunction<LVertex, LVertex> {

  /**
   * If true jitter positions of flattened LVertices.
   */
  private boolean jitter;

  /**
   * The factor k, used by the FRLayouter that layouted the vertices.
   */
  private double k;

  /**
   * Converts an LVertex (that can contain multiple sub-vertices into a list of single LVertices.
   *
   * @param jitter If true jitter positions of flattened LVertices.
   * @param k      The factor k, used by the FRLayouter that layouted the vertices.
   */
  public LVertexFlattener(boolean jitter, double k) {
    this.jitter = jitter;
    this.k = k;
  }

  @Override
  public void flatMap(LVertex superv, Collector<LVertex> collector) throws Exception {
    for (GradoopId id : superv.getSubVertices()) {
      double jitterRadius = 0;
      if (jitter) {
        jitterRadius = Math.sqrt(superv.getCount() * k);
      }
      LVertex v = new LVertex();
      v.setId(id);
      Vector position = superv.getPosition();
      if (jitter) {
        position = jitterPosition(position, jitterRadius);
      }
      v.setPosition(position);
      collector.collect(v);
    }
    superv.setSubVertices(null);
    collector.collect(superv);
  }

  /**
   * Add random jitter to position
   *
   * @param center Position
   * @param jitter Maximum distance
   * @return Randomly modified position
   */
  protected static Vector jitterPosition(Vector center, double jitter) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    Vector offset = new Vector();
    while (true) {
      double x = (rng.nextDouble(1) * jitter) - (jitter / 2.0);
      double y = (rng.nextDouble(1) * jitter) - (jitter / 2.0);
      offset.set(x, y);
      if (offset.magnitude() <= jitter) {
        break;
      }
    }
    return offset.mAdd(center);
  }
}
