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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.layouting.functions.AverageVertexPositionsFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Centroid;
import org.gradoop.flink.model.impl.operators.layouting.functions.CentroidRepulsionForceMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.CentroidUpdater;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.LVertexEPGMVertexJoinFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.SimpleGraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Layout a graph using approximate repulsive forces calculated using centroids as described
 * <a href="https://www.researchgate.net/publication/281348264_Distributed_Graph_Layout_with_Spark">
 * here</a>
 * Very fast, even for large inputs.
 */
public class CentroidFRLayouter extends FRLayouter {

  /**
   * Fraction of all vertices a centroid should minimally have
   */
  public static final double MIN_MASS_FACTOR = 0.0025d;
  /**
   * Fraction of all vertices a centroid should maximally have
   */
  public static final double MAX_MASS_FACTOR = 0.05d;
  /**
   * Name for the Centroid BroadcastSet
   */
  public static final String CENTROID_BROADCAST_NAME = "centroids";
  /**
   * Name for the Center BroadcastSet
   */
  public static final String CENTER_BROADCAST_NAME = "center";
  /**
   * DataSet containing the current centroids
   */
  private DataSet<Centroid> centroids;
  /**
   * DataSet containing the current graph-center
   */
  private DataSet<Vector> center;

  /**
   * Create new CentroidFRLayouter
   *
   * @param iterations  Number of iterations to perform
   * @param vertexCount Approximate number of vertices in the input-graph
   */
  public CentroidFRLayouter(int iterations, int vertexCount) {
    super(iterations, vertexCount);
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    g = createInitialLayout(g);

    DataSet<EPGMVertex> gradoopVertices = g.getVertices();
    DataSet<EPGMEdge> gradoopEdges = g.getEdges();

    DataSet<LVertex> vertices = gradoopVertices.map(LVertex::new);
    DataSet<LEdge> edges = gradoopEdges.map(LEdge::new);

    centroids = chooseInitialCentroids(vertices);

    // flink can only iterate over one dataset at once. Create a dataset containing both
    // centroids and vertices. Split them again at the begin of every iteration
    DataSet<SimpleGraphElement> graphElements = vertices.map(x -> x);
    graphElements = graphElements.union(centroids.map(x -> x));

    IterativeDataSet<SimpleGraphElement> loop = graphElements.iterate(iterations);
    vertices = loop.filter(x -> x instanceof LVertex).map(x -> (LVertex) x);
    centroids = loop.filter(x -> x instanceof Centroid).map(x -> (Centroid) x);

    centroids = calculateNewCentroids(centroids, vertices);
    center = calculateLayoutCenter(vertices);

    LGraph graph = new LGraph(vertices, edges);
    // we have overridden repulsionForces() so layout() will use or new centroid-based solution
    layout(graph);

    graphElements = graph.getVertices().map(x -> x);
    graphElements = graphElements.union(centroids.map(x -> x));

    graphElements = loop.closeWith(graphElements);

    vertices = graphElements.filter(x -> x instanceof LVertex).map(x -> (LVertex) x);

    gradoopVertices = vertices.join(gradoopVertices).where(LVertex.ID_POSITION).equalTo(new Id<>())
      .with(new LVertexEPGMVertexJoinFunction());

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  /* override and calculate repulsionFoces using centroids. Everything else stays like in the
  original FR */
  @Override
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    return vertices.map(new CentroidRepulsionForceMapper(new FRRepulsionFunction(getK())))
      .withBroadcastSet(centroids, CENTROID_BROADCAST_NAME)
      .withBroadcastSet(center, CENTER_BROADCAST_NAME);
  }

  /**
   * Randomly choose some vertex-positions as start centroids
   *
   * @param vertices Current (randomly placed) vertices of the graph
   * @return Random centroids to use (always at least one)
   */
  protected DataSet<Centroid> chooseInitialCentroids(DataSet<LVertex> vertices) {
    // Choose a sample rate that will statistically result in clusters with a mass exactly
    // between min and max allowed mass
    final double sampleRate =
      1.0 / (((MIN_MASS_FACTOR + MAX_MASS_FACTOR) / 2.0) * numberOfVertices);
    // Because of the randomness of the layouting it is possible that on small graphs no vertex
    // is chosen as centroid. This would result in problems. Therefore we union with one single
    // vertex, so there is ALWAYS at least one centroid
    return vertices.filter((v) -> Math.random() < sampleRate).union(vertices.first(1))
      .map(v -> new Centroid(v.getPosition(), 0));
  }

  /**
   * Calculate the current centroids for the graph
   *
   * @param centroids The old/current centroids
   * @param vertices  The current vertices of the graph
   * @return The new centroids (to use for the next iteration)
   */
  protected DataSet<Centroid> calculateNewCentroids(DataSet<Centroid> centroids,
    DataSet<LVertex> vertices) {

    CentroidUpdater updater =
      new CentroidUpdater(numberOfVertices, MIN_MASS_FACTOR, MAX_MASS_FACTOR);

    return updater.updateCentroids(centroids, vertices);
  }

  /**
   * Calculate the current center of the graph-layout
   *
   * @param vertices Current vertices of the graph
   * @return The average of all vertex positions
   */
  protected DataSet<Vector> calculateLayoutCenter(DataSet<LVertex> vertices) {
    return new AverageVertexPositionsFunction().averagePosition(vertices);
  }


  @Override
  public String toString() {
    return "CentroidFRLayouter{" + "iterations=" + iterations + ", k=" + getK() + ", width=" +
      getWidth() + ", height=" + getHeight() + ", numberOfVertices=" + numberOfVertices +
      ", useExistingLayout=" + useExistingLayout + '}';
  }
}
