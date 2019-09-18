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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.DefaultVertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexFusor;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.List;

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
     * Like EXTRACTED, but performs some more layouting-iterations after the extraction.
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
   * supervertex/superedge
   * are stored.
   */
  public static final String SUB_ELEMENTS_PROPERTY = "SUBELEMENTS";
  /**
   * Iterations used for the after-fusing layouting. (If OutputFormat.POSTLAYOUT is used.)
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
   * @param iterations  Number of iterations to perform
   * @param vertexCount Number of vertices in the input-graph (used to compute default-values)
   * @param threshold   nly vertices with a similarity of at least threshold are combined. Lower
   *                    values will lead to a more simplified output-graph. Valid values are >= 0
   *                    and <= 1
   * @param of          Chosen OutputFormat. See {@link OutputFormat}
   */
  public FusingFRLayouter(int iterations, int vertexCount, double threshold, OutputFormat of) {
    super(iterations, vertexCount);
    this.threshold = threshold;
    this.outputFormat = of;

    if (threshold < 0 || threshold > 1) {
      throw new IllegalArgumentException("Threshold must be between 0 and 1");
    }
  }

  /**
   * Sets optional value compareFunction. If no custom function is used
   * DefaultVertexCompareFunction will be used.
   *
   * @param compareFunction the new value
   * @return this (for method-chaining)
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
    DataSet<GraphElement> tmpvertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<GraphElement> tmpedges = gradoopEdges.map((e) -> new LEdge(e));
    DataSet<GraphElement> graphElements = tmpvertices.union(tmpedges);

    IterativeDataSet<GraphElement> loop = graphElements.iterate(
      (outputFormat != OutputFormat.POSTLAYOUT) ? iterations : iterations - POST_ITERATIONS);

    // split the combined dataset to work with the edges and vertices
    LGraph graph = new LGraph(loop);

    // perform the layouting
    layout(graph);

    // Use the VertexFusor to create a simplified version of the graph
    VertexFusor vf = new VertexFusor(getCompareFunction(), threshold);
    graph = vf.execute(graph);

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
      return null;
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
    final double kf = getK();
    DataSet<LVertex> vertices =
      graph.getVertices().flatMap((FlatMapFunction<LVertex, LVertex>) (superv, collector) -> {
        double jitterRadius = Math.sqrt(superv.getCount() * kf);
        for (GradoopId id : superv.getSubVertices()) {
          LVertex v = new LVertex();
          v.setId(id);
          v.setPosition(jitterPosition(superv.getPosition(), jitterRadius));
          collector.collect(v);
        }
        superv.setSubVertices(null);
        collector.collect(superv);
      }).returns(new TypeHint<LVertex>() {
      });

    DataSet<LEdge> edges = input.getEdges().map(e -> new LEdge(e));
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
      vertices.join(input.getVertices()).where(LVertex.ID).equalTo("id").with((lv, v) -> {
        lv.getPosition().setVertexPosition(v);
        return v;
      });

    return input.getFactory().fromDataSets(gradoopVertices, input.getEdges());
  }

  /**
   * Simply translate the internal representations into GRadoop-types
   *
   * @param input    Original input graph
   * @param layouted Result of the layouting
   * @return The layouted graph in the Gradoop-format
   */
  protected LogicalGraph buildSimplifiedGraph(LogicalGraph input, LGraph layouted) {
    DataSet<EPGMVertex> vertices = layouted.getVertices().map((lv) -> {
      EPGMVertex v = new EPGMVertex(lv.getId(), "vertex", new Properties(), null);
      lv.getPosition().setVertexPosition(v);
      v.setProperty(VERTEX_SIZE_PROPERTY, lv.getCount());
      v.setProperty(SUB_ELEMENTS_PROPERTY, getSubelementListValue(lv.getSubVertices()));
      return v;
    });

    DataSet<EPGMEdge> edges = layouted.getEdges().map((le) -> {
      EPGMEdge e =
        new EPGMEdge(le.getId(), "edge", le.getSourceId(), le.getTargetId(), new Properties(), null);
      e.setProperty(VERTEX_SIZE_PROPERTY, le.getCount());
      e.setProperty(SUB_ELEMENTS_PROPERTY, getSubelementListValue(le.getSubEdges()));
      return e;
    });
    return input.getFactory().fromDataSets(vertices, edges);
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
    final double kf = getK();
    DataSet<EPGMVertex> vertices =
      layouted.getVertices().flatMap((FlatMapFunction<LVertex, LVertex>) (superv, collector) -> {
        double jitterRadius = 0;
        if (jitter) {
          jitterRadius = Math.sqrt(superv.getCount() * kf);
        }
        for (GradoopId id : superv.getSubVertices()) {
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
      }).returns(new TypeHint<LVertex>() {
      }).join(input.getVertices()).where(LVertex.ID).equalTo("id").with((lv, v) -> {
        lv.getPosition().setVertexPosition(v);
        return v;
      });
    return input.getFactory().fromDataSets(vertices, input.getEdges());
  }

  /**
   * Add random jitter to position
   *
   * @param center Position
   * @param jitter Maximum distance
   * @return Randomly modified position
   */
  protected static Vector jitterPosition(Vector center, double jitter) {
    Vector offset = new Vector();
    while (true) {
      double x = (Math.random() * jitter) - (jitter / 2.0);
      double y = (Math.random() * jitter) - (jitter / 2.0);
      offset.set(x, y);
      if (offset.magnitude() <= jitter) {
        break;
      }
    }
    return offset.mAdd(center);
  }

  @Override
  protected DataSet<LVertex> applyForces(DataSet<LVertex> vertices, DataSet<Force> forces,
    int iterations) {
    return vertices.join(forces).where(LVertex.ID).equalTo(Force.ID).with(applicator);
  }

  /**
   * Helper function to convert the List of sub-elements into a comma seperated string
   * Gradoop (especially the CSVDataSink) seems to have trouble with lists of PropertyValues, so
   * this is the easies workaround
   *
   * @param ids List of GradoopIds
   * @return A comma seperated string of ids
   */
  protected static String getSubelementListValue(List<GradoopId> ids) {
    StringBuilder sb = new StringBuilder();
    for (GradoopId id : ids) {
      sb.append(id.toString());
      sb.append(",");
    }
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "FusingFRLayouter{" + "threshold=" + threshold + ", outputFormat=" + outputFormat +
      ", iterations=" + iterations + ", k=" + getK() + ", width=" + getWidth() + ", height=" +
      getHeight() + ", maxRepulsionDistance=" + getMaxRepulsionDistance() + ", numberOfVertices=" +
      numberOfVertices + ", useExistingLayout=" + useExistingLayout + '}';
  }
}
