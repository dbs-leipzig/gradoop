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
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MaxAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MinAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;

/**
 * Demo program that combines the grouping operator with
 *
 * (1) the subgraph operator extract vertices and edges by specified predicate functions and
 * (2) the transformation operator to modify vertex properties which are used for grouping
 */
public class Composition extends AbstractRunner {

  /**
   * Loads a social network graph from the specified location, applies vertex and edge predicates
   * and groups the resulting graph by vertex properties.
   *
   * args[0] - input path (CSV)
   * args[1] - output path
   *
   * @param args arguments
   * @throws IOException if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    // instantiate a default gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());

    // define a data source to load the graph
    DataSource dataSource = new CSVDataSource(inputPath, config);

    // load the graph
    LogicalGraph socialNetwork = dataSource.getLogicalGraph();

    // use the subgraph operator to filter the graph
    LogicalGraph subgraph =
      socialNetwork.subgraph(v -> v.getLabel().equals("person"), e -> e.getLabel().equals("knows"));

    // use the transformation operator to classify the 'birthday' property for the users

    LogicalGraph transformed = subgraph.transformVertices((current, modified) -> {
        LocalDate birthDayDate = current.getPropertyValue("birthday").getDate();
        current.setProperty("yob", birthDayDate.getYear());
        current.setProperty("decade", birthDayDate.getYear() - birthDayDate.getYear() % 10);
        return current;
      });

    // group the transformed graph by users decade and apply several aggregate functions
    LogicalGraph summary = transformed.groupBy(
        Collections.singletonList("decade"),
        Arrays.asList(
          new CountAggregator("count"),
          new MinAggregator("yob", "min_yob"),
          new MaxAggregator("yob", "max_yob")),
        Collections.emptyList(),
        Collections.singletonList(new CountAggregator("count")),
        GroupingStrategy.GROUP_COMBINE);

    // use the decade as label information for the DOT sink
    summary = summary.transformVertices((current, modified) -> {
        current.setLabel(current.getPropertyValue("decade").toString());
        return current;
      });

    // instantiate a data sink for the DOT format
    DataSink dataSink = new DOTDataSink(outputPath, false);
    dataSink.write(summary, true);

    getExecutionEnvironment().execute();

    // convert DOT to PNG image
    convertDotToPNG(outputPath, outputPath + ".png");
  }
}
