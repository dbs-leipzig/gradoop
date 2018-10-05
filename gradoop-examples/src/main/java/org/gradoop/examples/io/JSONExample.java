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
package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSink;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.singletonList;

/**
 * Example program that reads a graph from an EPGM-specific JSON representation
 * into a {@link GraphCollection}, does some computation and stores the
 * resulting {@link LogicalGraph} as JSON.
 *
 * In the JSON representation, an EPGM graph collection (or Logical Graph) is
 * stored in three (or two) separate files. Each line in those files contains
 * a valid JSON-document describing a single entity:
 *
 * Example graphHead (data attached to logical graphs):
 *
 * {
 *  "id":"graph-uuid-1",
 *  "data":{"interest":"Graphs","vertexCount":4},
 *  "meta":{"label":"Community"}
 * }
 *
 * Example vertex JSON document:
 *
 * {
 *  "id":"vertex-uuid-1",
 *  "data":{"gender":"m","city":"Dresden","name":"Dave","age":40},
 *  "meta":{"label":"Person","graphs":["graph-uuid-1"]}
 * }
 *
 * Example edge JSON document:
 *
 * {
 *  "id":"edge-uuid-1",
 *  "source":"14505ae1-5003-4458-b86b-d137daff6525",
 *  "target":"ed8386ee-338a-4177-82c4-6c1080df0411",
 *  "data":{},
 *  "meta":{"label":"friendOf","graphs":["graph-uuid-1"]}
 * }
 *
 * An example graph collection can be found under src/main/resources/data.json.
 * For further information, have a look at the {@link org.gradoop.flink.io.impl.deprecated.json}
 * package.
 */
public class JSONExample extends AbstractRunner implements ProgramDescription {

  /**
   * Reads an EPGM graph collection from a directory that contains the separate
   * files. Files can be stored in local file system or HDFS.
   *
   * args[0]: path to input graph
   * args[1]: path to output graph
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "provide graph/vertex/edge paths and output directory");
    }

    final String inputPath  = args[0];
    final String outputPath = args[1];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // create DataSource
    JSONDataSource dataSource = new JSONDataSource(inputPath, config);

    // read graph collection from DataSource
    GraphCollection graphCollection = dataSource.getGraphCollection();

    // do some analytics
    LogicalGraph schema = graphCollection
      .reduce(new ReduceCombination())
      .groupBy(singletonList(Grouping.LABEL_SYMBOL), singletonList(Grouping.LABEL_SYMBOL));

    // write resulting graph to DataSink
    schema.writeTo(new JSONDataSink(outputPath, config));

    // execute program
    env.execute();
  }

  @Override
  public String getDescription() {
    return "EPGM JSON IO Example";
  }
}
