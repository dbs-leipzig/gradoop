package org.gradoop.examples.benchmark;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanDecoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.decoders.GSpanGraphCollectionDecoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.WithCount;


import org.apache.commons.cli.CommandLine;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

public class TransactionalFSMBenchmark extends AbstractRunner implements
  ProgramDescription {


  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";
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
   * Used hdfs outputPath
   */
  private static String outputPath;

  private static boolean syn;

  private static boolean bulk;

  private static float minSupport;



  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
      "Group on edge labels");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path of the " +
      "generated CSV-File");
    OPTIONS.addOption(OPTION_SYNTHETIC, "syn", false, "Boolean synthetic flag");
    OPTIONS.addOption(OPTION_GSPAN_BULK, "bulkiteration", false, "Boolean " +
      "flag for BulkIteration or FilterRefine implementation");
    OPTIONS.addOption(OPTION_MIN_SUP, "minimum-support", true, "Minimum " +
      "Support");
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TransactionalFSMBenchmark.class
      .getName());

    if (cmd == null){
      return;
    }
    performSanityCheck(cmd);

    inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    minSupport =  Float.parseFloat(cmd.getOptionValue(OPTION_MIN_SUP));

    GradoopFlinkConfig gradoopConfig = GradoopFlinkConfig.createDefaultConfig(
      (getExecutionEnvironment()));

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> tlfSource = new
    TLFDataSource<>(inputPath, gradoopConfig);

    //Stephan source
    DataSet<TLFGraph> graphs = null;

    //Stephan encoder
    GSpanEncoder encoder = null;

    GSpanMiner miner;

    FSMConfig fsmConfig = syn ?
     new FSMConfig(minSupport, true, true) :
     new FSMConfig(minSupport, false, false);

    miner = bulk ? new GSpanBulkIteration() : new GSpanFilterRefine();

   //Stephan Datasource
    DataSet<WithCount<CompressedDFSCode>> result = null;

    DataSet<GSpanGraph> gsGraph = encoder.encode(graphs, fsmConfig);

    result = miner.mine(gsGraph,  ,fsmConfig);

    GSpanDecoder decoder = new GSpanGraphCollectionDecoder<>(gradoopConfig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection = decoder.decode(result, encoder
      .getVertexLabelDictionary(), encoder
      .getEdgeLabelDictionary());


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
      inputPath, syn,  bulk, minSupport,
      getExecutionEnvironment().getLastJobExecutionResult().getNetRuntime
        (TimeUnit.SECONDS));

    File f = new File(csvPath);
    if (f.exists() && !f.isDirectory()){
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


  }
}
