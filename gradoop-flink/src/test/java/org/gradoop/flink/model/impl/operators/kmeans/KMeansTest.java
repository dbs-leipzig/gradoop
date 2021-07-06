package org.gradoop.flink.model.impl.operators.kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.kmeans.util.*;
import org.gradoop.flink.model.impl.operators.kmeans.functions.*;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class KMeansTest<V extends Vertex> extends GradoopFlinkTestBase {

    private List<Tuple2<Centroid, Point>> expectedNearestCenter = new ArrayList<>();
    private List<Tuple3<Integer, Point, Long>> expectedNearestCenterWithAppendedCounter = new ArrayList<>();
    private List<Tuple3<Integer, Point, Long>> expectedCentroidsWithAccumulatedPoints = new ArrayList<>();
    private List<Centroid> expectedAverageCentroids = new ArrayList<>();
    String inputGdlGraph;
    String expectedOutputGdlGraph;

    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Point> points = env.fromElements(
            new Point(20.0, 10.0), new Point(25.0, 15.0), new Point(30.0, 20.0),
            new Point(35.0, 25.0), new Point(40.0, 30.0), new Point(10.0, 10.0),
            new Point(10.0, 30.0), new Point(20.0, 40.0), new Point(5.0, 10.0)
    );

    DataSet<Centroid> centroids = env.fromElements(
            new Centroid(1, new Point(10.0, 15.0)),
            new Centroid(2, new Point(20.0, 20.0)),
            new Centroid(3, new Point(25.0, 30.0))
            );

    @Before
    public void setUpTestVariables() {

        expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)), new Point(20.0, 10.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)), new Point(25.0, 15.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)), new Point(30.0, 20.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)), new Point(35.0, 25.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)), new Point(40.0, 30.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(1, new Point(10.0, 15.0)), new Point(10.0, 10.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(2, new Point(20.0, 20.0)), new Point(10.0, 30.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(3, new Point(25.0, 30.0)), new Point(20.0, 40.0)));
        expectedNearestCenter.add(new Tuple2<>(new Centroid(1, new Point(10.0, 15.0)), new Point(5.0, 10.0)));


        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(20.0, 10.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(25.0, 15.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(30.0, 20.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(35.0, 25.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(40.0, 30.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(1, new Point(10.0, 10.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(2, new Point(10.0, 30.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(3, new Point(20.0, 40.0), 1L));
        expectedNearestCenterWithAppendedCounter.add(new Tuple3<>(1, new Point(5.0, 10.0), 1L));

        expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(1, new Point(15.0, 20.0), 2L));
        expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(2, new Point(85.0, 75.0), 4L));
        expectedCentroidsWithAccumulatedPoints.add(new Tuple3<>(3, new Point(95.0, 95.0), 3L));

        expectedAverageCentroids.add(new Centroid(1, new Point(7.5, 10.0)));
        expectedAverageCentroids.add(new Centroid(2, new Point(21.25, 18.75)));
        expectedAverageCentroids.add(new Centroid(3, new Point(31.666666666666668, 31.666666666666668)));

        inputGdlGraph = "g[" +
                "(c1:Coordinate {lat: 10.0, long: 15.0})" +
                "(c2:Coordinate {lat: 20.0, long: 20.0})" +
                "(c3:Coordinate {lat: 25.0, long: 30.0})" +
                "(c4:Coordinate {lat: 20.0, long: 10.0})" +
                "(c5:Coordinate {lat: 25.0, long: 15.0})" +
                "(c6:Coordinate {lat: 30.0, long: 20.0})" +
                "(c7:Coordinate {lat: 35.0, long: 25.0})" +
                "(c8:Coordinate {lat: 40.0, long: 30.0})" +
                "(c9:Coordinate {lat: 10.0, long: 10.0})" +
                "(c10:Coordinate {lat: 10.0, long: 30.0})" +
                "(c11:Coordinate {lat: 20.0, long: 40.0})" +
                "(c12:Coordinate {lat: 5.0, long: 10.0})" +
                "]";
    }

    /*
    Unit Tests
     */
    @Test
    public void testSelectNearestCenter() throws Exception  {

        List<Tuple2<Centroid, Point>> nearestCenter =
                 points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids").collect();

        assertTrue(expectedNearestCenter.size() == nearestCenter.size() && expectedNearestCenter.containsAll(nearestCenter));
    }

    @Test
    public void testCountAppender() throws Exception {
        List<Tuple3<Integer, Point, Long>> nearestCenterWithAppendedCounter =
                 points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids").map(new CountAppender()).collect();

        assertTrue(expectedNearestCenterWithAppendedCounter.size() == nearestCenterWithAppendedCounter.size() &&
                expectedNearestCenterWithAppendedCounter.containsAll(nearestCenterWithAppendedCounter)
        );
    }

    @Test
    public void testCentroidAccumulator() throws Exception {
        List<Tuple3<Integer, Point, Long>> centroidsWithAccumulatedPoints =
                points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids").map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator()).collect();

        assertTrue(expectedCentroidsWithAccumulatedPoints.size() == centroidsWithAccumulatedPoints.size() &&
                expectedCentroidsWithAccumulatedPoints.containsAll(centroidsWithAccumulatedPoints));
    }

    @Test
    public void testCentroidAverager() throws Exception {
        List<Centroid> averageCentroids =
                points.map(new SelectNearestCenter()).withBroadcastSet(centroids, "centroids").map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator()).map(new CentroidAverager()).collect();

        assertTrue(expectedAverageCentroids.size() == averageCentroids.size() &&
                expectedAverageCentroids.containsAll(averageCentroids));
    }

    /*
    Integration Test
     */
    @Test
    public void testKMeansAlgorithm() throws Exception {
        LogicalGraph logicalGraph = getLoaderFromString(inputGdlGraph).getLogicalGraphByVariable("g");
        DataSet<V> vertices = new KMeans(3, 3).execute(logicalGraph).getVertices();

        List<String> test = vertices.map(v -> {
            String s=v.getProperties().toString() + "\n";
            return s;
        }).collect();

        System.out.println(test.toString());




    }
}
