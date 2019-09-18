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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.io.Serializable;
import java.util.List;

/**
 * Layout a graph using approximate repulsive forces calculated using centroids.
 * Very fast, even for large inputs.
 */
public class CentroidFRLayouter extends FRLayouter {

  /**
   * Fraction of all vertices a centroid should minimally have
   */
  protected static final double MIN_MASS_FACTOR = 0.0025d;
  /**
   * Fraction of all vertices a centroid should maximally have
   */
  protected static final double MAX_MASS_FACTOR = 0.05d;
  /**
   * Name for the Centroid BroadcastSet
   */
  protected static final String CENTROID_BROADCAST_NAME = "centroids";
  /**
   * Name for the Center BroadcastSet
   */
  protected static final String CENTER_BROADCAST_NAME = "center";
  /**
   * DataSet containing the current centroids
   */
  protected DataSet<Centroid> centroids;
  /**
   * DataSet containing the current graph-center
   */
  protected DataSet<Vector> center;

  /**
   * Create new CentroidFRLayouter
   * @param iterations Number of iterations to perform
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

    DataSet<LVertex> vertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<LEdge> edges = gradoopEdges.map((e) -> new LEdge(e));

    centroids = chooseInitialCentroids(vertices);

    // flink can only iterate over one dataset at once. Create a dataset containing both
    // centroids and vertices. Split them again at the begin of every iteration
    DataSet<GraphElement> graphElements = vertices.map(x -> x);
    graphElements = graphElements.union(centroids.map(x -> x));

    IterativeDataSet<GraphElement> loop = graphElements.iterate(iterations);
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

    gradoopVertices = vertices.join(gradoopVertices).where(LVertex.ID).equalTo("id")
      .with(new JoinFunction<LVertex, EPGMVertex, EPGMVertex>() {
        @Override
        public EPGMVertex join(LVertex lVertex, EPGMVertex vertex) throws Exception {
          lVertex.getPosition().setVertexPosition(vertex);
          return vertex;
        }
      });

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  /* override and calculate repulsionFoces using centroids. Everything else stays like in the
  original FR */
  @Override
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    FRRepulsionFunction rf = new FRRepulsionFunction(getK());
    return vertices.map(new RepulsionForceMapper(rf))
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

    CentroidUpdater upd = new CentroidUpdater(numberOfVertices);

    centroids = centroids.flatMap(upd::removeOrSplitCentroids);

    return vertices.map(upd).withBroadcastSet(centroids, CENTROID_BROADCAST_NAME).groupBy(Force.ID)
      .reduceGroup(upd::calculateNewCentroidPosition);
  }

  /**
   * Calculate the current center of the graph-layout
   *
   * @param vertices Current vertices of the graph
   * @return The average of all vertex positions
   */
  protected DataSet<Vector> calculateLayoutCenter(DataSet<LVertex> vertices) {
    return averagePosition(vertices);
  }

  /**
   * Helper function to calculate the average position of vertices
   *
   * @param vertices Input vertices
   * @return average position
   */
  protected static DataSet<Vector> averagePosition(DataSet<LVertex> vertices) {
    // combine local partition to make following reduce more efficient
    return vertices.combineGroup(new GroupCombineFunction<LVertex, Tuple2<Vector, Integer>>() {
      @Override
      public void combine(Iterable<LVertex> iterable,
        Collector<Tuple2<Vector, Integer>> collector) throws Exception {
        int count = 0;
        Vector sum = new Vector();
        for (LVertex v : iterable) {
          count++;
          sum.mAdd(v.getPosition());
        }
        collector.collect(new Tuple2<>(sum, count));
      }
      // reduce results from all partition into one
    }).reduce((a, b) -> {
      a.f0.mAdd(b.f0);
      a.f1 += b.f1;
      return a;
      // calculate the average
    }).map(t -> t.f0.mDiv(t.f1));
  }

  /**
   * Calculate repulsion-forces for vertices using centroids
   */
  protected static class RepulsionForceMapper extends RichMapFunction<LVertex, Force> {
    /**
     * Current centroids
     */
    protected List<Centroid> centroids;
    /**
     * Current center of the graph
     */
    protected List<Vector> center;
    /**
     * For object-reuse
     */
    protected LVertex centroidVertex = new LVertex();
    /**
     * The function to use for the calculation of the repulsion-force
     */
    protected FRRepulsionFunction rf;

    /**
     * Create new calculator
     *
     * @param rf Repulsion function to use
     */
    public RepulsionForceMapper(FRRepulsionFunction rf) {
      this.rf = rf;
    }

    /**
     * Constructor FOR TESTING! Provide centroids and center manually.
     *
     * @param rf        Repulsion function
     * @param centroids Centroids to use
     * @param center    Center of graph
     */
    public RepulsionForceMapper(FRRepulsionFunction rf, List<Centroid> centroids,
      List<Vector> center) {
      this.rf = rf;
      this.centroids = centroids;
      this.center = center;
      centroidVertex.setId(GradoopId.get());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      centroids = getRuntimeContext().getBroadcastVariable(CENTROID_BROADCAST_NAME);
      center = getRuntimeContext().getBroadcastVariable(CENTER_BROADCAST_NAME);
    }


    @Override
    public Force map(LVertex vertex) {
      Vector forceSum = new Vector();
      for (Centroid c : centroids) {
        centroidVertex.setId(c.getId());
        centroidVertex.setPosition(c.getPosition().copy());
        forceSum.mAdd(rf.join(vertex, centroidVertex).getValue());
      }
      centroidVertex.setPosition(center.get(0));
      forceSum.mAdd(rf.join(vertex, centroidVertex).getValue());
      return new Force(vertex.getId(), forceSum);
    }
  }

  /**
   * Bundles Operations to update the centroid dataset
   */
  protected static class CentroidUpdater extends RichMapFunction<LVertex, Force> {

    /**
     * Number of vertices in the graph
     */
    protected int vertexCount;
    /**
     * List of current centroids. Usually populated using broadcastVariables, but can be
     * populated manually for testing. Used for getClosestCentroidForVertex()
     */
    protected List<Centroid> centroids;

    /**
     * Create new updater
     *
     * @param vertexCount Number of vertices in the graph
     */
    public CentroidUpdater(int vertexCount) {
      this.vertexCount = vertexCount;
    }

    /**
     * Constructor FOR TESTING! Provide centroids manually (without broadcastVariable)
     *
     * @param vertexCount Number of vertices in the graph
     * @param centroids   List of current centroids
     */
    public CentroidUpdater(int vertexCount, List<Centroid> centroids) {
      this.vertexCount = vertexCount;
      this.centroids = centroids;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      if (getRuntimeContext().hasBroadcastVariable(CENTROID_BROADCAST_NAME)) {
        centroids = getRuntimeContext().getBroadcastVariable(CENTROID_BROADCAST_NAME);
      }
    }

    /**
     * Remove centroids that are to specific and split split centroids that are to general
     *
     * @param c         Current centroids
     * @param collector Collector for new centroids
     */
    public void removeOrSplitCentroids(Centroid c, Collector<Centroid> collector) {
      if (c.getCount() == 0) {
        collector.collect(c);
      } else if (c.getCount() < MIN_MASS_FACTOR * vertexCount) {
        // do nothing
      } else if (c.getCount() > MAX_MASS_FACTOR * vertexCount) {
        Centroid splitted = new Centroid(
          c.getPosition().add(new Vector(Math.random() * 2 - 1, Math.random() * 2 - 1)),
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

      if (centroids.size() == 0) {
        throw new IllegalStateException(
          "There are no centroids (left). This should " + "NEVER happen. Layouting failed...");
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
        throw new IllegalStateException("Ooops. This should never have happened...");
      }
      return best;
    }

    /**
     * Expects the group of vertex-positions for a centroid. Calculates the new position of the
     * centroid as average ov the vertex-positions
     *
     * @param iterable  Force-objects (id=centroidid, value=the position of the vertex)
     * @param collector The newly created centoid
     */
    public void calculateNewCentroidPosition(Iterable<Force> iterable,
      Collector<Centroid> collector) {
      int count = 0;
      Vector posSum = new Vector();
      for (Force f : iterable) {
        count++;
        posSum.mAdd(f.getValue());
      }
      collector.collect(new Centroid(posSum.mDiv(count), count));
    }

  }


  /**
   * Represents a centroid for repulsion-force computation
   */
  protected static class Centroid extends Tuple3<GradoopId, Vector, Integer> implements
    GraphElement, Serializable {


    /**
     * Create a new Centroid. Id is choosen automatically.
     *
     * @param position Position of the centroid
     * @param count    Number of vertices associated to the centroid
     */
    public Centroid(Vector position, int count) {
      super(GradoopId.get(), position, count);
    }

    /**
     * Default constructor to conform with POJO-Rules
     */
    public Centroid() {
      super();
    }

    /**
     * Gets position
     *
     * @return value of position
     */
    public Vector getPosition() {
      return f1;
    }

    /**
     * Sets position
     *
     * @param position the new value
     */
    public void setPosition(Vector position) {
      this.f1 = position;
    }

    /**
     * Gets count
     *
     * @return value of count
     */
    public int getCount() {
      return f2;
    }

    /**
     * Sets count
     *
     * @param count the new value
     */
    public void setCount(int count) {
      this.f2 = count;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public GradoopId getId() {
      return f0;
    }

    /**
     * Sets id
     *
     * @param id the new value
     */
    public void setId(GradoopId id) {
      this.f0 = id;
    }
  }

  @Override
  public String toString() {
    return "CentroidFRLayouter{" + "iterations=" + iterations + ", k=" + getK() + ", width=" +
      getWidth() + ", height=" + getHeight() + ", numberOfVertices=" + numberOfVertices +
      ", useExistingLayout=" + useExistingLayout + '}';
  }
}
