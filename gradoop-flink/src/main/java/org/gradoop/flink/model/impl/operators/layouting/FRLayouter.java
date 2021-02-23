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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRAttractionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdSelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.LVertexEPGMVertexJoinFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

/**
 * Layouts a graph using the Fruchtermann-Reingold algorithm
 */
public class FRLayouter implements LayoutingAlgorithm {

  /**
   * Default value for parameter k. All other default-values are derived from that.
   */
  protected static final double DEFAULT_K = 100;

  /**
   * Number of iterations to perform
   */
  protected int iterations;
  /**
   * User supplied k. Main-parameter of the FR-Algorithm. Optimum distance between connected
   * vertices.
   */
  protected double k = 0;
  /**
   * User supplied width of the layouting-space
   */
  protected int width = 0;
  /**
   * User supplied height of the layouting-space
   */
  protected int height = 0;
  /**
   * User supplied maximum distance for computing repulsion-forces between vertices
   */
  protected int maxRepulsionDistance = 0;
  /**
   * (Estimated) number of vertices in the graph. Needed to calculate default
   * parameters
   */
  protected int numberOfVertices;
  /**
   * If true, do not create a random initial layout but use the existing layout of the graph
   * instead.
   */
  protected boolean useExistingLayout = false;
  /**
   * Perform the layouting as if this number of iterations had already passed
   */
  private int startAtIteration = 0;


  /**
   * Create new Instance of FRLayouter.
   *
   * @param iterations  Number of iterations to perform
   * @param vertexCount (Estimated) number of vertices in the graph. Needed to calculate default
   *                    parammeters
   */
  public FRLayouter(int iterations, int vertexCount) {
    if (iterations <= 0 || vertexCount <= 0) {
      throw new IllegalArgumentException("Iterations and vertexcount must both be greater than 0.");
    }
    this.iterations = iterations;
    this.numberOfVertices = vertexCount;
  }


  /**
   * Override default k-parameter of the FR-Algorithm
   * Default: 100
   *
   * @param k new k
   * @return this (for method-chaining)
   */
  public FRLayouter k(double k) {
    if (k <= 0) {
      throw new IllegalArgumentException("K must be greater than 0.");
    }
    this.k = k;
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
  public FRLayouter area(int width, int height) {
    if (width <= 0 || height <= 0) {
      throw new IllegalArgumentException("Width and height must both be greater than 0.");
    }
    this.width = width;
    this.height = height;
    return this;
  }

  /**
   * Override default maxRepulsionDistance of the FR-Algorithm. Vertices with larger distance
   * are ignored in repulsion-force calculation
   * Default-Value is relative to current k. If k is overridden, this is changed
   * accordingly automatically.
   * Default: 2k
   *
   * @param maxRepulsionDistance new value
   * @return this (for method-chaining)
   */
  public FRLayouter maxRepulsionDistance(int maxRepulsionDistance) {
    if (maxRepulsionDistance <= 0) {
      throw new IllegalArgumentException("MaxRepulsionDistance must be greater than 0.");
    }
    this.maxRepulsionDistance = maxRepulsionDistance;
    return this;
  }

  /**
   * Use the existing layout as starting point instead of creating a random one.
   * If used, EVERY vertex in the input-graph MUST have an X and Y property!
   *
   * @param uel whether to re-use the existing layout or not
   * @return this (for method chaining)
   */
  public FRLayouter useExistingLayout(boolean uel) {
    this.useExistingLayout = uel;
    return this;
  }

  /**
   * Perform the layouting as if this number of iterations had already passed
   *
   * @param startAtIteration the number of previous iterations
   * @return this (for method-chaining)
   */
  public FRLayouter startAtIteration(int startAtIteration) {
    if (startAtIteration < 0) {
      throw new IllegalArgumentException("Start-Iteration must be greater than or equal to 0.");
    }
    this.startAtIteration = startAtIteration;
    return this;
  }

  /**
   * Gets k
   *
   * @return value of k
   */
  public double getK() {
    return (k != 0) ? k : DEFAULT_K;
  }


  @Override
  public int getWidth() {
    return (width != 0) ? width : (int) Math.sqrt(Math.pow(DEFAULT_K, 2) * numberOfVertices);
  }

  @Override
  public int getHeight() {
    return (height != 0) ? height : (int) Math.sqrt(Math.pow(DEFAULT_K, 2) * numberOfVertices);
  }

  /**
   * Gets maxRepulsionDistance
   *
   * @return value of maxRepulsionDistance
   */
  public int getMaxRepulsionDistance() {
    return (maxRepulsionDistance != 0) ? maxRepulsionDistance : (int) (2 * getK());
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    g = createInitialLayout(g);

    DataSet<EPGMVertex> gradoopVertices = g.getVertices();
    DataSet<EPGMEdge> gradoopEdges = g.getEdges();

    DataSet<LVertex> vertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<LEdge> edges = gradoopEdges.map((e) -> new LEdge(e));

    IterativeDataSet<LVertex> loop = vertices.iterate(iterations);
    LGraph graph = new LGraph(loop, edges);
    layout(graph);
    vertices = loop.closeWith(graph.getVertices());

    gradoopVertices = vertices
      .join(gradoopVertices)
      .where(LVertex.ID_POSITION).equalTo("id")
      .with(new LVertexEPGMVertexJoinFunction());

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  /**
   * Creates a layout as the starting-point for the algorithm. If useExistingLayout is false, the
   * created layout is random, else it is the already existing layout of the graph.
   *
   * @param g The graph to layout
   * @return The randomly layouted input graph
   */
  protected LogicalGraph createInitialLayout(LogicalGraph g) {
    if (useExistingLayout) {
      return g;
    }
    return new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
      getHeight() - (getHeight() / 10)).execute(g);
  }

  /**
   * Perform the actual layouting (calculate and apply forces)
   *
   * @param g The Graph to layout. It is modified by this method.
   */
  protected void layout(LGraph g) {
    DataSet<Force> repulsions = repulsionForces(g.getVertices());
    DataSet<Force> attractions = attractionForces(g.getVertices(), g.getEdges());

    DataSet<Force> forces = repulsions
      .union(attractions)
      .groupBy(Force.ID_POSITION)
      .reduce((first, second) -> {
        first.setValue(first.getValue().add(second.getValue()));
        return first;
      });

    g.setVertices(applyForces(g.getVertices(), forces, iterations));
  }

  /**
   * Applies the given forces to the given vertices.
   *
   * @param vertices   Vertices to move
   * @param forces     Forces to apply. At most one per vertex. The id indicates which vertex
   *                   the force should be applied to
   * @param iterations Number of iterations that will be performed (NOT the number of the
   *                   current iteration). It is used to compute the simulated annealing shedule.
   * @return The input vertices with x and y coordinated changed according to the given force and
   * current iteration number.
   */
  protected DataSet<LVertex> applyForces(DataSet<LVertex> vertices, DataSet<Force> forces,
    int iterations) {
    FRForceApplicator applicator =
      new FRForceApplicator(getWidth(), getHeight(), getK(), iterations);
    applicator.setPreviousIterations(startAtIteration);
    return vertices.join(forces).where(LVertex.ID_POSITION).equalTo(Force.ID_POSITION).with(applicator);
  }


  /**
   * Calculates the repulsive forces between the given vertices.
   *
   * @param vertices A dataset of vertices
   * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
   */
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    vertices = vertices.map(new FRCellIdMapper(getMaxRepulsionDistance()));

    KeySelector<LVertex, Integer> selfselector = new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRRepulsionFunction repulsionFunction = new FRRepulsionFunction(getK(), getMaxRepulsionDistance());

    DataSet<Force> self = vertices
      .join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF)).equalTo(selfselector)
      .with((JoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> up = vertices
      .join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UP)).equalTo(selfselector)
      .with((FlatJoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> left = vertices
      .join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT)).equalTo(selfselector)
      .with((FlatJoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> uright = vertices
      .join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT)).equalTo(selfselector)
      .with((FlatJoinFunction<LVertex, LVertex, Force>) repulsionFunction);

    DataSet<Force> uleft = vertices
      .join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT)).equalTo(selfselector)
      .with((FlatJoinFunction<LVertex, LVertex, Force>) repulsionFunction);


    return self.union(up).union(left).union(uright).union(uleft);
  }

  /**
   * Compute the attractive-forces between all vertices connected by edges.
   *
   * @param vertices The vertices
   * @param edges    The edges between vertices
   * @return A mapping from VertexId to x and y forces
   */
  protected DataSet<Force> attractionForces(DataSet<LVertex> vertices, DataSet<LEdge> edges) {
    return edges.join(vertices).where(LEdge.SOURCE_ID_POSITION).equalTo(LVertex.ID_POSITION).join(vertices)
      .where("f0." + LEdge.TARGET_ID_POSITION).equalTo(LVertex.ID_POSITION).with(
        (first, second) -> new Tuple3<LVertex, LVertex, Integer>(first.f1, second,
          first.f0.getCount())).returns(new TypeHint<Tuple3<LVertex, LVertex, Integer>>() {
            }).flatMap(new FRAttractionFunction(getK()));
  }

  @Override
  public String toString() {
    return "FRLayouter{" + "iterations=" + iterations + ", k=" + getK() + ", with=" + getWidth() +
      ", height=" + getHeight() + ", maxRepulsionDistance=" + getMaxRepulsionDistance() +
      ", numberOfVertices=" + numberOfVertices + ", useExistingLayout=" + useExistingLayout + '}';
  }
}
