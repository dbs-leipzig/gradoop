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
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * The benchmark program executes the following workflow:
 *
 * 1) Extract subgraph with:
 *    - vertex predicate: must be of type 'Person'
 *    - edge predicate: must be of type 'knows'
 * 2) Group the subgraph using the vertex attributes 'city' and 'gender' and
 *    - count the number of vertices represented by each super vertex
 *    - count the number of edges represented by each super edge
 * 3) Aggregate the grouped graph:
 *    - add the total vertex count as new graph property
 *    - add the total edge count as new graph property
 */
public class SNABenchmark1 extends AbstractRunner implements
  ProgramDescription {

  /**
   * Runs the example program.
   *
   * Need a (possibly HDFS) input directory that contains
   *  - nodes.json
   *  - edges.json
   *  - graphs.json
   *
   * Needs a (possibly HDFS) output directory to write the resulting graph to.
   *
   * @param args args[0] = input dir, args[1] output dir
   * @throws Exception
   */
  @SuppressWarnings({
    "unchecked",
    "Duplicates"
  })
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
      args.length == 2, "input dir and output dir required");
    String inputDir  = args[0];
    String outputDir = args[1];

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);

    LogicalGraph result = execute(epgmDatabase);

    writeLogicalGraph(result, outputDir);
  }

  /**
   * The actual computation.
   *
   * @param socialNetwork social network graph
   * @return summarized, aggregated graph
   */
  private static LogicalGraph
  execute(LogicalGraph socialNetwork) {
    return socialNetwork
      .subgraph(
        new FilterFunction<Vertex>() {
          @Override
          public boolean filter(Vertex vertex) throws Exception {
            return vertex.getLabel().equals("person");
          }
        },
        new FilterFunction<Edge>() {
          @Override
          public boolean filter(Edge edge) throws Exception {
            return edge.getLabel().equals("knows");
          }
        })
      .groupBy(Lists.newArrayList("gender", "city"))
      .aggregate("vertexCount", new VertexCount())
      .aggregate("edgeCount", new EdgeCount());
  }

  @Override
  public String getDescription() {
    return SNABenchmark1.class.getName();
  }
}
