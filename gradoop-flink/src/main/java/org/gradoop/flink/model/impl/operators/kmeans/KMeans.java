/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.kmeans.functions.SelectNearestCenter;
import org.gradoop.flink.model.impl.operators.kmeans.functions.VertexPostProcessingMap;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CountAppender;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CentroidAccumulator;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CentroidAverager;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

import java.util.Objects;

/**
 * Takes a logical graph, a user-defined amount of iterations and centroids, and the property names
 * of the vertex that are used for the clustering as input. Adds the clusterId, together with the
 * cluster coordinates to the properties of the vertex. The datatype of the properties can be any numeric
 * value. Returns the logical graph with modified vertex properties.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class KMeans<G extends GraphHead, V extends Vertex, E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>, GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Number of iterations
   */
  private final int iterations;

  /**
   * Amount of clusters
   */
  private final int centroids;

  /**
   * Name of the first spatial property used for the clustering
   */
  private final String firstPropertyName;

  /**
   * Name of the second spatial property used for the clustering
   */
  private final String secondPropertyName;

  /**
   * Constructor to create an instance of KMeans
   *
   * @param iterations        Number of iterations, e.g., 20
   * @param centroids         Amount of centroids that are determined by the algorithm
   * @param propertyNameOne   First spatial property name of the vertices
   * @param propertyNameTwo   Second spatial property name of the vertices
   */
  public KMeans(int iterations, int centroids, String propertyNameOne, String propertyNameTwo) {
    this.iterations = iterations;
    this.centroids = centroids;
    this.firstPropertyName = Objects.requireNonNull(propertyNameOne);
    this.secondPropertyName = Objects.requireNonNull(propertyNameTwo);
  }

  @Override
  public LG execute(LG logicalGraph) {

    final String lat = this.firstPropertyName;
    final String lon = this.secondPropertyName;

    DataSet<V> spatialVertices =
      logicalGraph.getVertices().filter(v -> v.hasProperty(lat) && v.hasProperty(lon));

    DataSet<Point> points = spatialVertices.map(v -> {
      double latValue = ((Number) v.getPropertyValue(lat).getObject()).doubleValue();
      double lonValue = ((Number) v.getPropertyValue(lon).getObject()).doubleValue();
      return new Point(latValue, lonValue);
    });

    DataSet<Tuple2<Long, Point>> indexingPoints = DataSetUtils.zipWithIndex(points.first(centroids));
    DataSet<Centroid> firstCentroids =
      indexingPoints.map(t -> new Centroid(Math.toIntExact(t.f0), t.f1.getLat(), t.f1.getLon()));

    IterativeDataSet<Centroid> loop = firstCentroids.iterate(iterations);

    DataSet<Centroid> newCentroids = points
      // Assigns a centroid to every vertex
      .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")

      // Add value 1 to prepare for grouping
      .map(new CountAppender())

      // Groups mapping by id and sums up points of every centroid. For every addition the count increments
      .groupBy(0).reduce(new CentroidAccumulator())

      // Divides summed up points through its counter and assigns the cluster a new centroid
      .map(new CentroidAverager());

    DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

    DataSet<Tuple2<Centroid, Point>> clusteredPoints =
      points.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

    DataSet<Tuple2<V, Tuple2<Centroid, Point>>> joinedVertices =
      logicalGraph.getVertices().join(clusteredPoints).where((KeySelector<V, Point>) v -> {
        double latValue = ((Number) v.getPropertyValue(lat).getObject()).doubleValue();
        double lonValue = ((Number) v.getPropertyValue(lon).getObject()).doubleValue();
        return new Point(latValue, lonValue);
      }).equalTo(1);

    DataSet<V> newVertices = joinedVertices.map(new VertexPostProcessingMap<>(lat, lon));

    return logicalGraph.getFactory()
      .fromDataSets(logicalGraph.getGraphHead(), newVertices, logicalGraph.getEdges());
  }
}
