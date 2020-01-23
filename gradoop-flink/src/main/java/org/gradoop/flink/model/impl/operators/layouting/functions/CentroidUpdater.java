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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.layouting.CentroidFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Centroid;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.List;

/**
 * Bundles Operations to update the centroid dataset of the CentroidFRLayouter.
 */
public class CentroidUpdater extends RichMapFunction<LVertex, Force> {
  /**
   * List of current centroids. Usually populated using broadcastVariables, but can be
   * populated manually for testing. Used for getClosestCentroidForVertex()
   */
  protected List<Centroid> centroids;
  /**
   * Fraction of all vertices a centroid should minimally have
   */
  private double minMassFactor;
  /**
   * Fraction of all vertices a centroid should minimally have
   */
  private double maxMassFactor;
  /**
   * Number of vertices in the graph
   */
  private int vertexCount;

  /**
   * Create new updater
   *
   * @param vertexCount Number of vertices in the graph
   * @param minMassFactor Factor of total vertices a centroid must have at least
   * @param maxMassFactor Factor of total vertices a centroid must have as most
   */
  public CentroidUpdater(int vertexCount, double minMassFactor, double maxMassFactor) {
    this.vertexCount = vertexCount;
    this.minMassFactor = minMassFactor;
    this.maxMassFactor = maxMassFactor;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (getRuntimeContext().hasBroadcastVariable(CentroidFRLayouter.CENTROID_BROADCAST_NAME)) {
      centroids = getRuntimeContext().getBroadcastVariable(CentroidFRLayouter.CENTROID_BROADCAST_NAME);
    }
  }

  /**
   * Update the centroids.
   *
   * @param centroids Current centroids
   * @param vertices Current vertices
   * @return Newly calculated centroids
   */
  public DataSet<Centroid> updateCentroids(DataSet<Centroid> centroids, DataSet<LVertex> vertices) {
    centroids = centroids.flatMap(this::removeOrSplitCentroids);
    return vertices.map(this)
      .withBroadcastSet(centroids, CentroidFRLayouter.CENTROID_BROADCAST_NAME).groupBy(Force.ID_POSITION)
      .reduceGroup(this::calculateNewCentroidPosition);
  }

  /**
   * Remove centroids that are to specific and split split centroids that are to general
   *
   * @param c         Current centroids
   * @param collector Collector for new centroids
   */
  protected void removeOrSplitCentroids(Centroid c, Collector<Centroid> collector) {
    if (c.getCount() == 0) {
      collector.collect(c);
    } else if (c.getCount() < minMassFactor * vertexCount) {
      // do nothing
    } else if (c.getCount() > maxMassFactor * vertexCount) {
      Centroid splitted =
        new Centroid(c.getPosition().add(new Vector(Math.random() * 2 - 1, Math.random() * 2 - 1)),
          c.getCount() / 2);
      c.setCount(c.getCount() / 2);
      collector.collect(c);
      collector.collect(splitted);
    } else {
      collector.collect(c);
    }
  }

  /**
   * For every vertex chooses the closest centroid.
   * The Force-class is abused here, because it bundles a GradoopId and a Vector (what is
   * exactly what we need here)
   * We can not give it a proper name and use method-references as then it would no be
   * * recognised as RichMapFunction.
   *
   * @param vertex The current vertices
   * @return A Force object with the id of the centroid and the position of the vertex.
   * @throws IllegalStateException If there are no centroids. (Therefore can not choose a closest
   *                               centroid.
   */
  public Force map(LVertex vertex) {

    if (centroids == null) {
      throw new IllegalStateException("DataSet of centroids MUST be broadcasted to this class");
    }

    if (centroids.size() == 0) {
      throw new IllegalStateException(
        "There are no centroids (left). This should NEVER happen. Layouting failed...");
    }

    Force best = new Force();
    double bestDist = Double.MAX_VALUE;
    for (Centroid c : centroids) {
      double dist = c.getPosition().distance(vertex.getPosition());
      if (dist < bestDist) {
        best.set(c.getId(), vertex.getPosition());
        bestDist = dist;
      }
    }
    if (best.getId() == null) {
      throw new IllegalStateException("There is no closest centroid. This means there " +
        "is a bug in this implementation, probably a NaN occured " +
        "during distance calculation.");
    }
    return best;
  }

  /**
   * Expects the group of vertex-positions for a centroid. Calculates the new position of the
   * centroid as average of the vertex-positions.
   * <p>
   * forceObjects does not really contain "forces", but it has the fields needed herre (id and
   * vector). The id of the force object represents the id of the centroid of which the new
   * position is calculated and the
   * force-vector is the position of a vertex belonging to the centroid.
   *
   * @param forceObjects List of vertex positions, wrapped in Force-objects.
   * @param collector    The newly created centoid
   */
  protected void calculateNewCentroidPosition(Iterable<Force> forceObjects,
    Collector<Centroid> collector) {
    int count = 0;
    Vector posSum = new Vector();
    for (Force f : forceObjects) {
      count++;
      posSum.mAdd(f.getValue());
    }
    collector.collect(new Centroid(posSum.mDiv(count), count));
  }
}
