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
package org.gradoop.utils.centrality;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.model.impl.operators.statistics.functions.CalculateDegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.functions.DegreeDistanceFunction;

/**
 * example class calculating degree centrality of a graph
 */
public class DegreeCentralityExample extends AbstractRunner implements ProgramDescription {

  /**
   * name of property key degree
   */
  private static final String DEGREE_KEY = "degree";

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    // read logical Graph
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);
    DataSet<Long> vertexCount = new VertexCount().execute(graph);

    // broadcasting
    DataSet<WithCount<GradoopId>> maxDegree = degrees.max(1);

    String broadcastName = "degree_max";
    DataSet<Double> degree = degrees
      .map(new DegreeDistanceFunction(broadcastName))
      .withBroadcastSet(maxDegree, broadcastName)
      .sum(0)
      .crossWithTiny(vertexCount).with(new CalculateDegreeCentrality());
    degree.print();
  }

  @Override
  public String getDescription() {
    return DegreeCentralityExample.class.getName();
  }
}
