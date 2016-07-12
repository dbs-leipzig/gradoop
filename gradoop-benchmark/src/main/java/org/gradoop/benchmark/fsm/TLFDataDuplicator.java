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

package org.gradoop.benchmark.fsm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tlf.TLFDataSink;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.functions.utils.Duplicate;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * A program to duplicate TLF data sets.
 */
public class TLFDataDuplicator
  extends AbstractRunner implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_MULTIPLICAND = "m";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH,
      "input-path", true, "path of graph files (hdfs)");
    OPTIONS.addOption(OPTION_MULTIPLICAND,
      "multiplicand", true, "number of duplicates per graph");
  }

  /**
   * Main program to run the duplications. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TLFDataDuplicator.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> config =
      GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment());

    String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    Integer multiplicand =
      Integer.valueOf(cmd.getOptionValue(OPTION_MULTIPLICAND));

    String outputPath = inputPath.replace(".tlf", "_" + multiplicand + ".tlf");

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(inputPath, config);

    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new TLFDataSink<>(outputPath, config);

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> input =
      dataSource.getGraphTransactions();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new GraphTransactions<>(
        input
          .getTransactions()
          .flatMap(new Duplicate
          <GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo>>
          (multiplicand))
          .returns(GraphTransaction.getTypeInformation(config)),
        config
      );

    dataSink.write(output);

    config.getExecutionEnvironment().execute();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Input file must be provided");
    }
    if (!cmd.hasOption(OPTION_MULTIPLICAND)) {
      throw new IllegalArgumentException("Multiplicand must be provided.");
    }
  }

  @Override
  public String getDescription() {
    return TLFDataDuplicator.class.getName();
  }
}
