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
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.singletonList;
import static org.gradoop.flink.model.impl.operators.grouping.Grouping.LABEL_SYMBOL;

/**
 * Demo program that uses graph grouping to extract the schema from a (possibly large) property
 * (logical) graph. The resulting summary graph is written to DOT for easy visualization.
 */
public class Schema extends AbstractRunner {

  /**
   * Loads the graph from the specified input path, computes its schema via grouping and writes
   * the result as DOT into the output path and converts it into an PNG image.
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

    // load the graph
    LogicalGraph graph = dataSource.getLogicalGraph();

    // use graph grouping to extract the schema
    LogicalGraph schema = graph.groupBy(singletonList(LABEL_SYMBOL), singletonList(LABEL_SYMBOL));

    // instantiate a data sink for the DOT format
    DataSink dataSink = new DOTDataSink(outputPath, false);
    dataSink.write(schema, true);

    // run the job
    getExecutionEnvironment().execute();

    // convert to PNG
    convertDotToPNG(outputPath, outputPath + ".png");
  }
}
