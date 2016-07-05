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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable
  .PredictableTransactionsGenerator;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;

import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.GSpanTLFGraphEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;

import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;


import org.apache.commons.cli.CommandLine;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized graph fsm benchmark.
 */
public class TransactionalFSMBenchmark
  extends AbstractRunner
  implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Path to CSV log file
   */
  private static final String OPTION_CSV_PATH = "csv";
  /**
   * Synthetic dataset flag
   */
  private static final String OPTION_SYNTHETIC = "syn";
  /**
   * BulkIteration or FilterRefine flag
   */
  private static final String OPTION_GSPAN_BULK = "bulk";
  /**
   * Value of minimum supported frequency
   */
  private static final String OPTION_MIN_SUP = "ms";
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used hdfs INPUT_PATH
   */
  private static String INPUT_PATH;
  /**
   * Minimum Supported frequency
   */
  private static float MIN_SUPPORT;
  /**
   * Flag if the dataset is a real world dataset or not
   */
  private static boolean SYNTHETIC_FLAG;
  /**
   * Flag witch encoding algorithm should be used
   */
  private static boolean BULK_ITERATION_FLAG;


  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true,
      "Path to graph files (hdfs)");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path of the " +
      "generated CSV-File");
    OPTIONS.addOption(OPTION_SYNTHETIC, "syn", false, "Boolean synthetic flag");
    OPTIONS.addOption(OPTION_GSPAN_BULK, "bulk iteration", false, "Boolean " +
      "flag for BulkIteration or FilterRefine implementation");
    OPTIONS.addOption(OPTION_MIN_SUP, "minimum-support", true, "Minimum " +
      "Support");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args,
      TransactionalFSMBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read cmd arguments
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    MIN_SUPPORT = Float.parseFloat(cmd.getOptionValue(OPTION_MIN_SUP));
    SYNTHETIC_FLAG = cmd.hasOption(OPTION_SYNTHETIC);
    BULK_ITERATION_FLAG = cmd.hasOption(OPTION_GSPAN_BULK);

    // create gradoop conf
    GradoopFlinkConfig gradoopConfig =
      GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment());

    // read tlf graph
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> tlfSource =
      new TLFDataSource<>(INPUT_PATH, gradoopConfig);

    // create input dataset
    DataSet<TLFGraph> graphs = tlfSource.getTLFGraphs();

    // set encoder
    GSpanEncoder encoder = new GSpanTLFGraphEncoder<>();

    // set miner
    GSpanMiner miner;

    miner = BULK_ITERATION_FLAG ? new GSpanBulkIteration() :
      new GSpanFilterRefine();

    miner.setExecutionEnvironment(getExecutionEnvironment());

    // set config for synthetic or real world dataset
    FSMConfig fsmConfig = SYNTHETIC_FLAG ?
      new FSMConfig(MIN_SUPPORT, true, true) :
      new FSMConfig(MIN_SUPPORT, false, false);

    // encode
    DataSet<GSpanGraph> gsGraph = encoder.encode(graphs, fsmConfig);

    // mine
    DataSet<WithCount<CompressedDFSCode>>
      countDataSet = miner.mine(gsGraph, encoder.getMinFrequency(), fsmConfig);

    long actualCount = countDataSet.count();
    long expectedCount =
      PredictableTransactionsGenerator.containedFrequentSubgraphs(MIN_SUPPORT);

    String resultMessage = actualCount == expectedCount ?
      "met expected result" :
      "expected " + expectedCount +
        " but found " + actualCount + " frequent subgraphs";

    System.out.println(resultMessage);

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

    String head = String.format("%s|%s|%s|%s|%s|%s%n", "Parallelism",
      "dataset", "synthetic", "bulk", "MIN_SUPPORT", "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s%n",
      getExecutionEnvironment().getParallelism(), INPUT_PATH, SYNTHETIC_FLAG,
      BULK_ITERATION_FLAG, MIN_SUPPORT,
      getExecutionEnvironment().getLastJobExecutionResult()
        .getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return TransactionalFSMBenchmark.class.getName();
  }
}
