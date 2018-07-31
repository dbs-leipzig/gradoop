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
package org.gradoop.examples.dimspan;

import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A program to scale up graph data in TLF format by copying single graphs n times.
 *
 * TLF-format:
 *
 * t # <graphId>
 * v <id> <label>
 * ..
 * e <source> <target> <label>
 * ..
 */
public class TLFDataCopier extends AbstractRunner implements ProgramDescription {

  /**
   * Option to set path to input file
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to set path to output file
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to set number of copies per graphs
   */
  public static final String COPY_COUNT = "c";

  /**
   * Gradoop configuration
   */
  private static GradoopFlinkConfig GRADOOP_CONFIG =
    GradoopFlinkConfig.createConfig(getExecutionEnvironment());

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true, "Path to input file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true, "Path to output file");
    OPTIONS.addOption(COPY_COUNT, "copy-count", true, "Number of copies per input graph");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TLFDataCopier.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    int copyCount = Integer.parseInt(cmd.getOptionValue(COPY_COUNT));

    // Create data source and sink
    DataSource dataSource = new TLFDataSource(inputPath, GRADOOP_CONFIG);
    DataSink dataSink = new TLFDataSink(outputPath, GRADOOP_CONFIG);

    DataSet<GraphTransaction> input = dataSource
      .getGraphCollection()
      .getGraphTransactions();

    DataSet<GraphTransaction> output = input
      .flatMap(
        (FlatMapFunction<GraphTransaction, GraphTransaction>) (graphTransaction, collector) -> {
          for (int i = 1; i <= copyCount; i++) {
            collector.collect(graphTransaction);
          }
        }
      )
      .returns(TypeExtractor
        .getForObject(new GraphTransaction(new GraphHead(), Sets.newHashSet(), Sets.newHashSet())));

    // execute and write to disk
    dataSink.write(
      GRADOOP_CONFIG.getGraphCollectionFactory().fromTransactions(output),
      true);
    getExecutionEnvironment().execute();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("No input file specified.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("No output file specified.");
    }
    if (!cmd.hasOption(COPY_COUNT)) {
      throw new IllegalArgumentException("No copy count specified.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return TLFDataCopier.class.getName();
  }
}
