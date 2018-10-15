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
package org.gradoop.examples.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.util.GradoopFlinkConfig;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * An example for the subgraph operator using combined filter functions.
 *
 * @see org.gradoop.flink.model.impl.operators.subgraph.Subgraph
 * @see org.gradoop.flink.model.impl.functions.filters.CombinableFilter
 */
public class SubgraphExample {
  /**
   * Run an example subgraph operation on the example social media graph.
   *
   * @param args The arguments (unused).
   * @throws Exception if the execution fails.
   */
  public static void main(String[] args) throws Exception {
    // Get the Flink ExecutionEnvironment.
    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    // Create Gradoop config.
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(executionEnvironment);
    // Read the input graph.
    LogicalGraph inputGraph = new CSVDataSource(
      URLDecoder.decode(SubgraphExample.class.getResource("/data/csv/sna").getFile(),
        StandardCharsets.UTF_8.name()), config).getLogicalGraph();

    // Create a filter for vertices accepting:
    // 1. Vertices with label "Forum" and a property "title" set to "Graph Processing"
    // 2. Vertices with label "Person"
    FilterFunction<Vertex> vertexFilter = new ByLabel<Vertex>("Forum")
      .and(new ByProperty<>("title", PropertyValue.create("Graph Processing")))
      .or(new ByLabel<>("Person"));
    // Create a filter for edges accepting: Edges with label "knows" or "hasModerator"
    // with the "since" property not set to 2015.
    FilterFunction<Edge> edgeFilter = new ByLabel<Edge>("knows")
      .or(new ByLabel<>("hasModerator"))
      .and(new ByProperty<>("since", PropertyValue.create(2015)).negate());

    // Use these filters with a subgraph call:
    LogicalGraph filteredGraph = inputGraph.subgraph(vertexFilter, edgeFilter);

    // Print result graph
    filteredGraph.print();
  }
}
