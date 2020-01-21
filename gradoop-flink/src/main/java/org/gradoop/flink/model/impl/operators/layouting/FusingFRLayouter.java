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
import org.gradoop.flink.model.impl.operators.layouting.functions.DefaultVertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.LGraphToEPGMMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.LVertexEPGMVertexJoinFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.LVertexFlattener;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexFusor;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.SimpleGraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;


/**
 * A special variant of the FRLayouter that combines similar vertices during the layouting,
 * creating a simplified version of the graph.
 */
public class FusingFRLayouter extends FRLayouter {

  /**
   * Specifies the different available Output-Formats for the layouter
   */
  public enum OutputFormat {
    /**
     * Output the simplified graph. The output-graph will loose all information except for the
     * GradoopIds. Vertices/Edges will have "SUBELEMENTS"-Property containing a comma
     * separated string listing all element-ids that were
     * combined into the super-element. Edges and Vertices will have a "SIZE"-Property containing
     * the number of sub-elements contained in this super-element.
     */
    SIMPLIFIED,
    /**
     * Grouped vertices will be resolved into the original vertices and will be placed randomly
     * close to another.
     */
    EXTRACTED,
    /**
     * Like EXTRACTED, but vertices of a supernode will be placed at exactly the same position.
     */
    RAWEXTRACTED,
    /**
     * Like EXTRACTED, but performs some more layouting-iterations after the extraction. At least 11
     * iterations are needed to used POSTLAYOUT.
     */
    POSTLAYOUT
  }

  /**
   * Name of the property that will contain the number of sub-vertices or sub-edges for a vertex or
   * edge
   */
  public static final String VERTEX_SIZE_PROPERTY = "SIZE";
  /**
   * The name of the property where the ids of the sub-vertices (or sub-edges) of a
   * supervertex/superedge are stored.
   */
  public static final String SUB_ELEMENTS_PROPERTY = "SUBELEMENTS";
  /**
   * Iterations used for the after-fusing layouting. (If {@link OutputFormat#POSTLAYOUT} is used.)
   */
  private static final int POST_ITERATIONS = 10;
  /**
   * Only vertices with a similarity of at least threshold are combined
   */
  protected double threshold;
  /**
   * Compare function to use. Null means use default.
   */
  protected VertexCompareFunction compareFunction = null;
  /**
   * The output format chosen by the user
   */
  protected OutputFormat outputFormat;
  /**
   * The force-applicator used by layout(). Can be modified to change the layout behavior.
   */
  protected FRForceApplicator applicator;

  /**
   * Create new FusingFRLayouter
   *
   * @param iterations   Number of iterations to perform
   * @param vertexCount  Number of vertices in the input-graph (used to compute default-values)
   * @param threshold    Only vertices with a similarity of at least threshold are combined. Lower
   *                     values will lead to a more simplified output-graph. Valid values are {@code
   *                     >= 0 }
   *                     and {@code <= 1 }.
   * @param outputFormat Chosen OutputFormat. See {@link OutputFormat}
   */
  public FusingFRLayouter(int iterations, int vertexCount, double threshold,
    OutputFormat outputFormat) {
    super(iterations, vertexCount);

    if (threshold < 0 || threshold > 1) {
      throw new IllegalArgumentException("Threshold must be between 0 and 1");
    }

    if (outputFormat == OutputFormat.POSTLAYOUT && iterations <= POST_ITERATIONS) {
      throw new IllegalArgumentException("When using OutputFormat.POSTLAYOUT, at least 11 " +
        "iterations are needed");
    }

    this.threshold = threshold;
    this.outputFormat = outputFormat;
  }

  /**
   * Sets optional value compareFunction. If no custom function is used
   * DefaultVertexCompareFunction will be used.
   *
   * @param compareFunction the new value
   * @return this layouter
   */
  public FusingFRLayouter compareFunction(VertexCompareFunction compareFunction) {
    this.compareFunction = compareFunction;
    return this;
  }

  /**
   * Gets compareFunction
   *
   * @return value of compareFunction
   */
  public VertexCompareFunction getCompareFunction() {
    return (compareFunction != null) ? compareFunction : new DefaultVertexCompareFunction(getK());
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    applicator = new FRForceApplicator(getWidth(), getHeight(), getK(),
      (outputFormat != OutputFormat.POSTLAYOUT) ? iterations : iterations - POST_ITERATIONS);

    g = createInitialLayout(g);

    DataSet<EPGMVertex> gradoopVertices = g.getVertices();
    DataSet<EPGMEdge> gradoopEdges = g.getEdges();

    // Flink can only iterate over a single dataset. Therefore vertices and edges have to be
    // temporarily combined into a single dataset.
    // Also the Grapdoop datatypes are converted to internal datatypes
    DataSet<SimpleGraphElement> tmpvertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<SimpleGraphElement> tmpedges = gradoopEdges.map((e) -> new LEdge(e));
    DataSet<SimpleGraphElement> graphElements = tmpvertices.union(tmpedges);

    IterativeDataSet<SimpleGraphElement> loop = graphElements.iterate(
      (outputFormat != OutputFormat.POSTLAYOUT) ? iterations : iterations - POST_ITERATIONS);

    // split the combined dataset to work with the edges and vertices
    LGraph graph = new LGraph(loop);

    // perform the layouting
    layout(graph);

    // Use the VertexFusor to create a simplified version of the graph
    graph = new VertexFusor(getCompareFunction(), threshold).execute(graph);

    // again, combine vertices and edges into a single dataset to perform iterations
    graphElements = graph.getGraphElements();
    graphElements = loop.closeWith(graphElements);

    // again, split the combined dataset  (after all iterations have been completed)
    graph = new LGraph(graphElements);


    switch (outputFormat) {
    case SIMPLIFIED:
      return buildSimplifiedGraph(g, graph);
    case EXTRACTED:
      return buildExtractedGraph(g, graph, true);
    case RAWEXTRACTED:
      return buildExtractedGraph(g, graph, false);
    case POSTLAYOUT:
      return buildPostLayoutGraph(g, graph);
    default:
      throw new IllegalArgumentException("Unsupported output-format");
    }

  }

  /**
   * Extract all subverties/subedges from the super-vertices/super-edges and place them at the
   * location of the super-vertex (and add some random jitter to the positions).
   * Then some more layouting-iteraions are performed.
   *
   * @param input Original input graph
   * @param graph Result of the layouting
   * @return The final graph, containing all vertices and edges from the original graph.
   */
  protected LogicalGraph buildPostLayoutGraph(LogicalGraph input, LGraph graph) {
    DataSet<LVertex> vertices = graph.getVertices().flatMap(new LVertexFlattener(true, getK()));

    DataSet<LEdge> edges = input.getEdges().map(LEdge::new);
    graph.setEdges(edges);


    // use a new applicator for all following layouting iterations. The new applicator will
    // behave as if iteration x of n is actually iterations+x of n+POST_ITERATIONS
    applicator =
      new FRForceApplicator(getWidth(), getHeight(), getK(), iterations + POST_ITERATIONS);
    applicator.setPreviousIterations(iterations);

    // do some more layouting iterations
    IterativeDataSet<LVertex> loop = vertices.iterate(POST_ITERATIONS);
    graph.setVertices(loop);
    layout(graph);
    vertices = loop.closeWith(graph.getVertices());


    DataSet<EPGMVertex> gradoopVertices =
      vertices.join(input.getVertices()).where(LVertex.ID_POSITION).equalTo("id")
        .with(new LVertexEPGMVertexJoinFunction());

    return input.getFactory().fromDataSets(gradoopVertices, input.getEdges());
  }

  /**
   * Simply translate the internal representations back to {@link LogicalGraph}.
   *
   * @param input    Original input graph
   * @param layouted Result of the layouting
   * @return The layouted graph in the Gradoop-format
   */
  protected LogicalGraph buildSimplifiedGraph(LogicalGraph input, LGraph layouted) {
    return new LGraphToEPGMMapper().buildSimplifiedGraph(input, layouted);
  }

  /**
   * Extract all subverties/subedges from the super-vertices/super-edges and place them at the
   * location of the super-vertex (and add some random jitter to the positions)
   *
   * @param input    Original input graph
   * @param layouted Result of the layouting
   * @param jitter   Enable/disable adding jitter to subvertex positions
   * @return The final graph, containing all vertices and edges from the original graph.
   */
  protected LogicalGraph buildExtractedGraph(LogicalGraph input, LGraph layouted,
    final boolean jitter) {

    DataSet<EPGMVertex> vertices =
      layouted.getVertices().flatMap(new LVertexFlattener(jitter, getK())).join(input.getVertices())
        .where(LVertex.ID_POSITION).equalTo(new Id<>()).with(new LVertexEPGMVertexJoinFunction());
    return input.getFactory().fromDataSets(vertices, input.getEdges());
  }

  @Override
  protected DataSet<LVertex> applyForces(DataSet<LVertex> vertices, DataSet<Force> forces, int iterations) {
    return vertices.join(forces).where(LVertex.ID_POSITION).equalTo(Force.ID_POSITION).with(applicator);
  }


  @Override
  public String toString() {
    return "FusingFRLayouter{" + "threshold=" + threshold + ", outputFormat=" + outputFormat +
      ", iterations=" + iterations + ", k=" + getK() + ", width=" + getWidth() + ", height=" +
      getHeight() + ", maxRepulsionDistance=" + getMaxRepulsionDistance() + ", numberOfVertices=" +
      numberOfVertices + ", useExistingLayout=" + useExistingLayout + '}';
  }
}
