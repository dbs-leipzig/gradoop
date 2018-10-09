/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.examples.sna;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;

import java.util.Arrays;

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
   * Need a (possibly HDFS) input directory that contains a EPGM graph
   * in a given format (csv, indexed, json)
   *
   * Needs a (possibly HDFS) output directory to write the resulting graph to.
   *
   * @param args args[0] = input dir, args[1] input format, args[2] output dir
   * @throws Exception
   */
  @SuppressWarnings({
    "unchecked",
    "Duplicates"
  })
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
      args.length == 3, "input dir, input format and output dir required");
    String inputDir  = args[0];
    String inputFormat = args[1];
    String outputDir = args[2];

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir, inputFormat);

    LogicalGraph result = execute(epgmDatabase);

    writeLogicalGraph(result, outputDir);
  }

  /**
   * The actual computation.
   *
   * @param socialNetwork social network graph
   * @return summarized, aggregated graph
   */
  private static LogicalGraph execute(LogicalGraph socialNetwork) {
    return socialNetwork
      .subgraph(
        vertex -> vertex.getLabel().equals("Person"),
        edge -> edge.getLabel().equals("knows"))
      .groupBy(Arrays.asList("gender", "city"))
      .aggregate(new VertexCount(), new EdgeCount());
  }

  @Override
  public String getDescription() {
    return SNABenchmark1.class.getName();
  }
}
