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
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;

/**
 * Example program that reads a graph from an EPGM format (csv, indexed, json)
 * into a {@link GraphCollection} and stores the
 * resulting {@link GraphCollection} as DOT.
 * The resulting format is described in {@link DOTFileFormat}.
 */
public class DOTExample extends AbstractRunner implements ProgramDescription {

  /**
   * Reads an EPGM graph collection from a directory that contains the separate
   * files. Files can be stored in local file system or HDFS.
   *
   * args[0]: path to graph files
   * args[1]: input graph format
   * args[2]: path to write output graph
   * args[3]: flag to write graph head information
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      throw new IllegalArgumentException(
        "provide graph/vertex/edge paths, output directory and flag to print " +
          "graph head information (true/false)");
    }

    final String inputDir             = args[0];
    final String inputFormat          = args[1];
    final String outputDir            = args[2];
    final String graphHeadInformation = args[3];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // read graph collection
    GraphCollection collection = readGraphCollection(inputDir, inputFormat);

    // create DataSink
    DOTDataSink dataSink =
      new DOTDataSink(outputDir, Boolean.parseBoolean(graphHeadInformation));

    // write dot format
    dataSink.write(collection);

    // execute program
    env.execute();
  }

  @Override
  public String getDescription() {
    return DOTExample.class.getName();
  }
}
