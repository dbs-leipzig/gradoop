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

package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

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
 * For further information, have a look at the {@link org.gradoop.flink.io.impl.json}
 * package.
 */
public class JSONExample extends AbstractRunner implements ProgramDescription {

  /**
   * Reads an EPGM graph collection from a directory that contains the separate
   * files. Files can be stored in local file system or HDFS.
   *
   * args[0]: path to graph head file
   * args[1]: path to vertex file
   * args[2]: path to edge file
   * args[3]: path to write output graph
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
        "provide graph/vertex/edge paths and output directory");
    }

    final String graphHeadFile  = args[0];
    final String vertexFile     = args[1];
    final String edgeFile       = args[2];
    final String outputDir      = args[3];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // create DataSource
    JSONDataSource dataSource = new JSONDataSource(
      graphHeadFile, vertexFile, edgeFile, config);

    // read graph collection from DataSource
    GraphCollection graphCollection = dataSource.getGraphCollection();

    // do some analytics
    LogicalGraph schema = graphCollection
      .reduce(new ReduceCombination())
      .groupByVertexAndEdgeLabel();

    // write resulting graph to DataSink
    schema.writeTo(new JSONDataSink(
      outputDir + "graphHeads.json",
      outputDir + "vertices.json",
      outputDir + "edges.json",
      config));

    // execute program
    env.execute();
  }

  @Override
  public String getDescription() {
    return "EPGM JSON IO Example";
  }
}
