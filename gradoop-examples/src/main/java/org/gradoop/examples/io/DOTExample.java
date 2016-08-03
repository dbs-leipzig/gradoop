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
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program that reads a graph from an EPGM-specific JSON representation
 * into a {@link org.gradoop.flink.model.impl.GraphCollection} and stores the
 * resulting {@link org.gradoop.flink.model.impl.GraphCollection} as DOT.
 * The resulting format is described in {@link DOTFileFormat}.
 */
public class DOTExample extends AbstractRunner implements ProgramDescription {

  /**
   * Reads an EPGM graph collection from a directory that contains the separate
   * files. Files can be stored in local file system or HDFS.
   *
   * args[0]: path to graph head file
   * args[1]: path to vertex file
   * args[2]: path to edge file
   * args[3]: path to write output graph
   * args[4]: flag to write graph head information
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      throw new IllegalArgumentException(
        "provide graph/vertex/edge paths, output directory and flag to print " +
          "graph head information (true/false)");
    }

    final String graphHeadFile         = args[0];
    final String vertexFile            = args[1];
    final String edgeFile              = args[2];
    final String outputDir             = args[3];
    final String graphHeadInformation  = args[4];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // create DataSource
    JSONDataSource dataSource =
      new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);

    // create DataSink
    DOTDataSink dataSink =
      new DOTDataSink(outputDir, Boolean.parseBoolean(graphHeadInformation));

    // write dot format
    dataSink.write(dataSource.getGraphCollection());

    // execute program
    env.execute();
  }

  @Override
  public String getDescription() {
    return DOTExample.class.getName();
  }
}
