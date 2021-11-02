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
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CentroidAccumulator;
import org.gradoop.flink.model.impl.operators.kmeans.functions.SelectNearestCenter;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CountAppender;
import org.gradoop.flink.model.impl.operators.kmeans.functions.CentroidAverager;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link KMeans}.
 */
public class KMeansTest extends GradoopFlinkTestBase {

  private String inputGdlGraph;
  private DataSet<Point> points;
  private DataSet<Centroid> centroids;

  @Before
  public void setUpTestVariables() {

    points = getExecutionEnvironment().fromElements(new Point(20.0, 10.0), new Point(25.0, 15.0),
            new Point(30.0, 20.0), new Point(35.0, 25.0),
            new Point(40.0, 30.0), new Point(10.0, 10.0),
            new Point(10.0, 30.0), new Point(20.0, 40.0),
            new Point(5.0, 10.0));

    centroids = getExecutionEnvironment().fromElements(new Centroid(1, new Point(10.0, 15.0)),
            new Centroid(2, new Point(20.0, 20.0)),
            new Centroid(3, new Point(25.0, 30.0)));

    inputGdlGraph =
      "g[" + "(c1:Coordinate {lat: 10.0, long: 15.0})" + "(c2:Coordinate {lat: 20.0, long: 20.0})" +
        "(c3:Coordinate {lat: 25.012F, long: 30.012F})" + "(c4:Coordinate {lat: 20L, long: 10L})" +
        "(c5:Coordinate {lat: 25, long: 15})" + "(c6:Coordinate {lat: 30.0, long: 20.0})" +
        "(c7:Coordinate {lat: 35.0, long: 25.0})" + "(c8:Coordinate {lat: 40.0, long: 30.0})" +
        "(c9:Coordinate {lat: 10.0, long: 10.0})" + "(c10:Coordinate {lat: 10.0, long: 30.0})" +
        "(c11:Coordinate {lat: 20.0, long: 40.0})" + "(c12:Coordinate {lat: 5.0, long: 10.0})" + "]";
  }

  /*
  Unit Tests
   */
  @Test
  public void testSelectNearestCenter() throws Exception {

    List<Tuple2<Centroid, Point>> expectedNearestCenter = new ArrayList<>();
    expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)),
            new Point(20.0, 10.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)),
            new Point(25.0, 15.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)),
            new Point(30.0, 20.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)),
            new Point(35.0, 25.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)),
            new Point(40.0, 30.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(1, new Point(10.0, 15.0)),
            new Point(10.0, 10.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)),
            new Point(10.0, 30.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)),
            new Point(20.0, 40.0)));
    expectedNearestCenter.add(new Tuple2<>(new Centroid(1, new Point(10.0, 15.0)),
            new Point(5.0, 10.0)));

    List<Tuple2<Centroid, Point>> nearestCenter = new ArrayList<>();

    points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids").output(
      new LocalCollectionOutputFormat<>(nearestCenter)
    );

    getExecutionEnvironment().execute();

    assertTrue(expectedNearestCenter.size() == nearestCenter.size() &&
      expectedNearestCenter.containsAll(nearestCenter));
  }

  @Test
  public void testCountAppender() throws Exception {

    List<Tuple3<Integer, Point, Long>> expectedNearestCenterWithAppendedCounter = new ArrayList<>();
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(20.0, 10.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(25.0, 15.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(30.0, 20.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(35.0, 25.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(40.0, 30.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(1, new Point(10.0, 10.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(10.0, 30.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(20.0, 40.0), 1L));
    expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(1, new Point(5.0, 10.0), 1L));

    List<Tuple3<Integer, Point, Long>> nearestCenterWithAppendedCounter = new ArrayList<>();

    points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids")
      .map(new CountAppender()).output(new LocalCollectionOutputFormat<>(nearestCenterWithAppendedCounter));

    getExecutionEnvironment().execute();

    assertTrue(expectedNearestCenterWithAppendedCounter.size() ==
      nearestCenterWithAppendedCounter.size() &&
      expectedNearestCenterWithAppendedCounter.containsAll(nearestCenterWithAppendedCounter));
  }

  @Test
  public void testCentroidAccumulator() throws Exception {

    List<Tuple3<Integer, Point, Long>> expectedCentroidsWithAccumulatedPoints = new ArrayList<>();
    expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(1, new Point(15.0, 20.0), 2L));
    expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(2, new Point(85.0, 75.0), 4L));
    expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(3, new Point(95.0, 95.0), 3L));

    List<Tuple3<Integer, Point, Long>> centroidsWithAccumulatedPoints = new ArrayList<>();

    points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids")
      .map(new CountAppender()).groupBy(0).reduce(new CentroidAccumulator())
      .output(new LocalCollectionOutputFormat<>(centroidsWithAccumulatedPoints));

    getExecutionEnvironment().execute();

    assertTrue(expectedCentroidsWithAccumulatedPoints.size() ==
      centroidsWithAccumulatedPoints.size() &&
      expectedCentroidsWithAccumulatedPoints.containsAll(centroidsWithAccumulatedPoints));
  }

  @Test
  public void testCentroidAverager() throws Exception {

    List<Centroid> expectedAverageCentroids = new ArrayList<>();
    expectedAverageCentroids.add(new Centroid(1, new Point(7.5, 10.0)));
    expectedAverageCentroids.add(new Centroid(2, new Point(21.25, 18.75)));
    expectedAverageCentroids.add(new Centroid(3, new Point(31.666666666666668, 31.666666666666668)));
    List<Centroid> averageCentroids = new ArrayList<>();

    points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids")
      .map(new CountAppender()).groupBy(0).reduce(new CentroidAccumulator()).map(new CentroidAverager())
      .output(new LocalCollectionOutputFormat<>(averageCentroids));

    getExecutionEnvironment().execute();

    assertTrue(expectedAverageCentroids.size() == averageCentroids.size() &&
      expectedAverageCentroids.containsAll(averageCentroids));
  }

  /*
  Integration Test
   */
  @Test
  public void testKMeansAlgorithm() throws Exception {
    LogicalGraph logicalGraph = getLoaderFromString(inputGdlGraph).getLogicalGraphByVariable("g");
    LogicalGraph output =
      new KMeans<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>(20, 3, "lat", "long")
        .execute(logicalGraph);
    DataSet<EPGMVertex> vertices = output.getVertices();
    DataSet<EPGMVertex> comparisonVertices = vertices.filter(
      vertex -> vertex.hasProperty("cluster_lat") && vertex.hasProperty("cluster_long") &&
        vertex.hasProperty("cluster_id") && vertex.hasProperty("long") &&
        vertex.hasProperty("lat"));

    List<EPGMVertex> verticesList = new ArrayList<>();
    List<EPGMVertex> comparisonVerticesList = new ArrayList<>();

    vertices.output(new LocalCollectionOutputFormat<>(verticesList));
    comparisonVertices.output(new LocalCollectionOutputFormat<>(comparisonVerticesList));

    getExecutionEnvironment().execute();

    assertEquals(comparisonVerticesList.size(), verticesList.size());
  }
}
