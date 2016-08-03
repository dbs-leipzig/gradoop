/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.sna;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.examples.utils.ExampleOutput;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.algorithms.labelpropagation.GellyLabelPropagation;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * The program executes the following workflow:
 *
 * 1) Extract subgraph with:
 *    - vertex predicate: must be of type 'Person'
 *    - edge predicate: must be of type 'knows'
 * 2) Transform vertices and edges to necessary information
 * 3) Compute communities using Gelly label propagation
 * 4) Compute vertex count per community
 * 5) Select communities with a vertex count greater than a given threshold
 * 6) Combine the remaining graphs to a single graph
 * 7) Group the graph using the vertex attributes 'city' and 'gender' and
 *    - count the number of vertices represented by each super vertex
 *    - count the number of edges represented by each super edge
 * 8) Aggregate the grouped graph:
 *    - add the total vertex count as new graph property
 *    - add the total edge count as new graph property
 *
 * The program can be either executed using external data (for benchmarking) or
 * demo data ({@link #main(String[])}).
 */
public class SNABenchmark2 extends AbstractRunner implements
  ProgramDescription {

  /**
   * Runs the benchmark program.
   *
   * The program can be executed using either external data or demo data.
   *
   * If no arguments are given, the program is executed on a demo social network
   * which is described in 'resources/data/gdl/sna.gdl'.
   *
   * For using external data, the following arguments are mandatory:
   *
   * 1) (possibly HDFS) input directory that contains
   *  - nodes.json
   *  - edges.json
   *  - graphs.json
   *
   * 2) (possibly HDFS) output directory to write the resulting graph to
   *
   * 3) Threshold for community selection depending on the dataset size:
   *
   * Scale - Threshold (recommended)
   * 1     -     1,000
   * 10    -     7,500
   * 100   -    50,000
   * 1K    -   350,000
   * 10K   - 2,450,000
   *
   * @param args args[0]: input dir, args[1]: output dir, args[2]: threshold
   * @throws Exception
   */
  @SuppressWarnings({
    "unchecked",
    "Duplicates"
  })
  public static void main(String[] args) throws Exception {

    boolean useExternalData = args.length > 0;

    if (useExternalData) {
      executeWithExternalData(args);
    } else {
      executeWithDemoData(GradoopFlinkConfig
        .createConfig(ExecutionEnvironment.getExecutionEnvironment()));
    }
  }

  /**
   * Runs the benchmark program with external data (e.g. from HDFS)
   *
   * @param args args[0]: input dir, args[1]: output dir, args[2]: threshold
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private static void executeWithExternalData(String[] args) throws Exception {
    Preconditions.checkArgument(
      args.length == 3, "input dir, output dir and threshold required");
    String inputDir  = args[0];
    String outputDir = args[1];
    int threshold    = Integer.parseInt(args[2]);

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);

    writeLogicalGraph(execute(epgmDatabase, threshold), outputDir);

    getExecutionEnvironment().execute();
  }

  /**
   * Runs the benchmark with demo data.
   *
   * @param gradoopConf gradoop config
   * @throws Exception
   */
  private static void executeWithDemoData(GradoopFlinkConfig gradoopConf)
      throws Exception {
    ExampleOutput out = new ExampleOutput();

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(gradoopConf);

    String graphDefinition = IOUtils.toString(SNABenchmark2.class
      .getResourceAsStream("/data/gdl/sna.gdl"));

    loader.initDatabaseFromString(graphDefinition);

    LogicalGraph inputGraph = loader.getLogicalGraphByVariable("db");

    out.add("Input Graph", inputGraph);

    LogicalGraph outputGraph = execute(inputGraph, 2);

    out.add("Output Graph", outputGraph);

    out.print();
  }

  /**
   * The actual computation.
   *
   * @param socialNetwork social network graph
   * @param threshold     used in community selection predicate
   * @return summarized, aggregated graph
   */
  private static LogicalGraph
  execute(LogicalGraph socialNetwork,
    final int threshold) {

    final int maxIterations   = 4;
    final String vertexCount  = "vertexCount";
    final String edgeCount    = "edgeCount";
    final String person       = "person";
    final String knows        = "knows";
    final String city         = "city";
    final String gender       = "gender";
    final String birthday     = "birthday";
    final String label        = "label";

    return socialNetwork
      // 1) extract subgraph
      .subgraph(new FilterFunction<Vertex>() {
        @Override
        public boolean filter(Vertex vertex) throws Exception {
          return vertex.getLabel().toLowerCase().equals(person);
        }
      }, new FilterFunction<Edge>() {
        @Override
        public boolean filter(Edge edge) throws Exception {
          return edge.getLabel().toLowerCase().equals(knows);
        }
      })
      // project to necessary information
      .transform(new TransformationFunction<GraphHead>() {
        @Override
        public GraphHead execute(GraphHead current,
          GraphHead transformed) {
          return current;
        }
      }, new TransformationFunction<Vertex>() {
        @Override
        public Vertex execute(Vertex current, Vertex transformed) {
          transformed.setLabel(current.getLabel());
          transformed.setProperty(city, current.getPropertyValue(city));
          transformed.setProperty(gender, current.getPropertyValue(gender));
          transformed.setProperty(label, current.getPropertyValue(birthday));
          return transformed;
        }
      }, new TransformationFunction<Edge>() {
        @Override
        public Edge execute(Edge current, Edge transformed) {
          transformed.setLabel(current.getLabel());
          return transformed;
        }
      })
      // 3a) compute communities
      .callForGraph(new GellyLabelPropagation(maxIterations, label))
      // 3b) separate communities
      .splitBy(label)
      // 4) compute vertex count per community
      .apply(new ApplyAggregation(vertexCount, new VertexCount()))
      // 5) select graphs with more than minClusterSize vertices
      .select(new FilterFunction<GraphHead>() {
        @Override
        public boolean filter(GraphHead g) throws Exception {
          return g.getPropertyValue(vertexCount).getLong() > threshold;
        }
      })
      // 6) reduce filtered graphs to a single graph using combination
      .reduce(new ReduceCombination())
      // 7) group that graph by vertex properties
      .groupBy(Lists.newArrayList(city, gender))
      // 8a) count vertices of grouped graph
      .aggregate(vertexCount, new VertexCount())
      // 8b) count edges of grouped graph
      .aggregate(edgeCount, new EdgeCount());
  }

  @Override
  public String getDescription() {
    return SNABenchmark2.class.getName();
  }
}
