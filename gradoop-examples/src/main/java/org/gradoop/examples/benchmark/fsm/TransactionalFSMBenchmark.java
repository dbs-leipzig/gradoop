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

package org.gradoop.examples.benchmark.fsm;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.GSpanGraphTransactionsEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;


import org.apache.commons.cli.CommandLine;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

public class TransactionalFSMBenchmark
  extends AbstractRunner
  implements ProgramDescription {
  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Path to CSV log file
   */
  public static final String OPTION_CSV_PATH = "csv";
  /**
   * Synthetic dataset flag
   */
  public static final String OPTION_SYNTHETIC = "syn";
  /**
   * BulkIteration or FilterRefine flag
   */
  public static final String OPTION_GSPAN_BULK = "bulk";

  public static final String OPTION_MIN_SUP = "ms";
  /**
   * Used csv path
   */
  private static String csvPath;
  /**
   * Used hdfs inputPath
   */
  private static String inputPath;
  /**
   * Minimum Supported frequency
   */
  private static float minSupport;
  /**
   * Flag if the dataset is a real world dataset or not
   */
  private static boolean synFlag;
  /**
   * Flag witch encoding algorithm should be used
   */
  private static boolean bulkFlag;



  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path of the " +
      "generated CSV-File");
    OPTIONS.addOption(OPTION_SYNTHETIC, "syn", false, "Boolean synthetic flag");
    OPTIONS.addOption(OPTION_GSPAN_BULK, "bulk iteration", false, "Boolean " +
      "flag for BulkIteration or FilterRefine implementation");
    OPTIONS.addOption(OPTION_MIN_SUP, "minimum-support", true, "Minimum " +
      "Support");
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TransactionalFSMBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read cmd arguments
    inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    csvPath = cmd.getOptionValue(OPTION_CSV_PATH);
    minSupport = Float.parseFloat(cmd.getOptionValue(OPTION_MIN_SUP));
    synFlag = cmd.hasOption(OPTION_SYNTHETIC);
    bulkFlag = cmd.hasOption(OPTION_SYNTHETIC);

    // create gradoop conf
    GradoopFlinkConfig gradoopConfig =
      GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment());

    // read tlf graph
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> tlfSource =
      new TLFDataSource<>(inputPath, gradoopConfig);

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphs =
      tlfSource.getGraphTransactions();

    // set encoder
    GSpanEncoder encoder = new GSpanGraphTransactionsEncoder();

    // set miner
    GSpanMiner miner;

    miner = bulkFlag ? new GSpanBulkIteration() : new GSpanFilterRefine();

    // set config for synthetic or real world dataset
    FSMConfig fsmConfig = synFlag ? new FSMConfig(minSupport, true, true) :
      new FSMConfig(minSupport, false, false);

    // encode
    DataSet<GSpanGraph> gsGraph = encoder.encode(graphs, fsmConfig);

    // mine
    miner.mine(gsGraph, encoder.getMinFrequency(), fsmConfig);

    // execute
    getExecutionEnvironment().execute();

    // write statistics
    writeCSV();

  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File needed");
    }
    if (!cmd.hasOption(OPTION_MIN_SUP)) {
      throw new IllegalArgumentException("Minimum supp must not be null");
    }
  }

  /**
   * Method to create and add lines to a csv-file
   * @throws IOException
   */
  private static void writeCSV() throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s\n", "Parallelism",
      "dataset", "synthetic", "bulk", "minSupport", "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s\n",
      getExecutionEnvironment().getParallelism(),
      inputPath, synFlag,  bulkFlag, minSupport,
      getExecutionEnvironment().getLastJobExecutionResult().getNetRuntime
        (TimeUnit.SECONDS));

    File f = new File(csvPath);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  @Override
  public String getDescription() {
    return TransactionalFSMBenchmark.class.getName();
  }
}
