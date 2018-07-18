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
package org.gradoop.examples.grouping;

import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.gelly.labelpropagation.GellyLabelPropagation;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Demo program that uses {@link GellyLabelPropagation} to compute communities in a social network
 * and groups the vertices by their community identifier. The result is a summary graph where each
 * vertex represents a community including its user count while each edge represents all friendships
 * between communities.
 */
public class Communities extends AbstractRunner {

  /**
   * Loads a social network graph from the specified location, applies label propagation to extract
   * communities and computes a summary graph using the community id. The resulting summary graph is
   * written using the DOT format and also converted into an PNG image using GraphViz.
   *
   * args[0] - input path (CSV)
   * args[1] - output path
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    // instantiate a default gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());

    // define a data source to load the graph
    DataSource dataSource = new CSVDataSource(inputPath, config);

    // property key used for label propagation
    final String communityKey = "c_id";

    // load the graph and set initial community id
    LogicalGraph graph = dataSource.getLogicalGraph();
    graph = graph.transformVertices((current, transformed) -> {
        current.setProperty(communityKey, current.getId());
        return current;
      });

    // apply label propagation to compute communities
    graph = graph.callForGraph(new GellyLabelPropagation(10, communityKey));

    // group the vertices of the graph by their community, count vertices per group and edges
    // between groups
    LogicalGraph communities = graph.groupBy(singletonList(communityKey), // vertex grouping keys
      singletonList(new CountAggregator("count")), // vertex aggregate functions
      emptyList(), // edge grouping keys
      singletonList(new CountAggregator("count")), // edge aggregate functions
      GroupingStrategy.GROUP_REDUCE);

    // extract vertex induced subgraph only containing communities with more than 10 users
    communities = communities
      .vertexInducedSubgraph((vertex) -> vertex.getPropertyValue("count").getLong() > 10);

    // instantiate a data sink for the DOT format
    DataSink dataSink = new DOTDataSink(outputPath, false);
    dataSink.write(communities, true);

    // run the job
    getExecutionEnvironment().execute();

    // convert to png
    convertDotToPNG(outputPath, outputPath + ".png");
  }
}
