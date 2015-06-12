package org.gradoop.rdf.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.ConnectedComponentsComputation;
import org.gradoop.algorithms.PairAggregator;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.drivers.BulkLoadDriver;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.io.formats.EPGHBaseVertexInputFormat;
import org.gradoop.io.formats.GenericPairWritable;
import org.gradoop.io.formats.SubgraphExtractionVertexOutputFormat;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.model.Vertex;
import org.gradoop.model.operators.VertexAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RDF Analysis Driver
 */
public class RDFAnalysisDriver extends Configured implements Tool {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(RDFAnalysisDriver.class);

  /**
   * Job names start with that prefix.
   */
  private static final String JOB_PREFIX = "RDF Analysis: ";

  static {
    Configuration.addDefaultResource("giraph-site.xml");
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * Constructor
   */
  public RDFAnalysisDriver() {
    new ConfUtils();
  }

  /**
   * Starting point for RDF analysis pipeline.
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    CommandLine cmd = ConfUtils.parseArgs(args);
    if (cmd == null) {
      return 0;
    }

    String tablePrefix = cmd.getOptionValue(ConfigurationUtils
      .OPTION_TABLE_PREFIX, "");
    String tableVerticesName = tablePrefix + GConstants.DEFAULT_TABLE_VERTICES;
    boolean verbose = cmd.hasOption(ConfUtils.OPTION_VERBOSE);

    // Bulk load data
    if (!cmd.hasOption(ConfUtils.OPTION_SKIP_IMPORT)) {
      String[] options = prepareOptions(cmd);
      if (!prepareGraphStoreAndBulkLoad(conf, options)) {
        return -1;
      }
    }

    /*
    Step 2: CC Computation using Giraph
     */
    int workers =
      Integer.parseInt(cmd.getOptionValue(ConfUtils.OPTION_WORKERS));
    if (!runRDFComputation(conf, workers, tableVerticesName, verbose)) {
      return -1;
    }

    /*
    Step 3: Select And Aggregate using MapReduce
     */
    int scanCache = Integer.parseInt(
      cmd.getOptionValue(ConfUtils.OPTION_HBASE_SCAN_CACHE, "500"));
    int reducers = Integer.parseInt(
      cmd.getOptionValue(ConfUtils.OPTION_REDUCERS));
    if (!runSelectAndAggregate(conf, scanCache, reducers, tableVerticesName,
      tablePrefix + GConstants.DEFAULT_TABLE_GRAPHS, verbose)) {
      return -1;
    }

    return 0;
  }

  /**
   * Create custom options for the bulk load process.
   * @param cmd command line options
   * @return string array containing options
   */
  private String[] prepareOptions(CommandLine cmd) {
    List<String> argsList = new ArrayList<>();
    argsList.add("-" + BulkLoadDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER);
    argsList.add(RDFReader.class.getCanonicalName());
    argsList.add("-" + ConfUtils.OPTION_GRAPH_INPUT_PATH);
    argsList.add(cmd.getOptionValue(ConfUtils.OPTION_GRAPH_INPUT_PATH));
    argsList.add("-" + ConfUtils.OPTION_GRAPH_OUTPUT_PATH);
    argsList.add(cmd.getOptionValue(ConfUtils.OPTION_GRAPH_OUTPUT_PATH));
    if (cmd.hasOption(ConfUtils.OPTION_DROP_TABLES)) {
      argsList.add("-" + ConfUtils.OPTION_DROP_TABLES);
    }
    if (cmd.hasOption(ConfUtils.OPTION_VERBOSE)) {
      argsList.add("-" + ConfUtils.OPTION_VERBOSE);
    }
    argsList.add("-" + ConfUtils.OPTION_TABLE_PREFIX);
    argsList.add(cmd.getOptionValue(ConfUtils.OPTION_TABLE_PREFIX));

    return argsList.toArray(new String[argsList.size()]);
  }

  /**
   * Imports the data set to the given vertices table.
   *
   * @param conf       Cluster config
   * @param argOptions options needed for the bulk load
   * @return true, if the job completed successfully, false otherwise
   * @throws java.io.IOException
   */
  private boolean prepareGraphStoreAndBulkLoad(Configuration conf,
    String[] argOptions) throws Exception {
    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);
    int bulkExitCode = bulkLoadDriver.run(argOptions);

    if (bulkExitCode != 0) {
      LOG.error("Error during bulk import ... stopping pipeline");
      return false;
    }
    return true;
  }

  /**
   * Runs the computation on the input graph using Giraph.
   *
   * @param conf        Cluster configuration
   * @param workerCount Number of workers Giraph shall use
   * @param verticesTableName specify (custom) table vertices
   * @param verbose     print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private boolean runRDFComputation(Configuration conf, int workerCount,
    String verticesTableName, boolean verbose) throws
    IOException, ClassNotFoundException, InterruptedException {
    // set HBase table to read graph from
    conf.set(TableInputFormat.INPUT_TABLE, verticesTableName);
    // just scan necessary CFs (no properties needed)
    String columnFamiliesToScan = String
      .format("%s %s %s %s", GConstants.CF_META, GConstants.CF_OUT_EDGES,
        GConstants.CF_IN_EDGES, GConstants.CF_GRAPHS);
    conf.set(TableInputFormat.SCAN_COLUMNS, columnFamiliesToScan);
    // set HBase table to write computation results to
    conf.set(TableOutputFormat.OUTPUT_TABLE, verticesTableName);

    // setup Giraph job
    GiraphJob job = new GiraphJob(conf,
        JOB_PREFIX + ConnectedComponentsComputation.class.getName());
    GiraphConfiguration giraphConf = job.getConfiguration();

    giraphConf.setComputationClass(ConnectedComponentsComputation.class);
    giraphConf.setVertexInputFormatClass(EPGHBaseVertexInputFormat.class);
    giraphConf.setBoolean(EPGHBaseVertexInputFormat.READ_INCOMING_EDGES, true);
    giraphConf.setVertexOutputFormatClass(
      SubgraphExtractionVertexOutputFormat.class);
    giraphConf.setWorkerConfiguration(workerCount, workerCount, 100f);

    // assuming local environment
    if (workerCount == 1) {
      GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf, false);
      GiraphConstants.LOCAL_TEST_MODE.set(giraphConf, true);
    }

    return job.run(verbose);
  }

  /**
   * Runs Selection and Aggregation using a single MapReduce Job.
   *
   * @param conf      Cluster configuration
   * @param scanCache HBase client scan cache
   * @param reducers  number of reducers to use for the job
   * @param verticesTableName specify (custom) table vertices
   * @param graphsTableName specify (custom) table graphs
   * @param verbose   print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private boolean runSelectAndAggregate(Configuration conf, int scanCache,
    int reducers, String verticesTableName, String graphsTableName, boolean
    verbose) throws IOException, ClassNotFoundException, InterruptedException {
    /*
     mapper settings
     */
    conf.setClass(GConstants.VERTEX_HANDLER_CLASS, EPGVertexHandler.class,
      VertexHandler.class);
    // vertex predicate
    conf.setClass(SelectAndAggregate.VERTEX_PREDICATE_CLASS,
      SameAsPredicate.class, VertexPredicate.class);
    // vertex aggregate
    conf.setClass(SelectAndAggregate.VERTEX_AGGREGATE_CLASS,
      ComponentAggregate.class, VertexAggregate.class);

    /*
    reducer settings
     */
    conf.setClass(GConstants.GRAPH_HANDLER_CLASS, EPGGraphHandler.class,
      GraphHandler.class);
    conf.setClass(SelectAndAggregate.PAIR_AGGREGATE_CLASS,
      ComponentCountAggregator.class, PairAggregator.class);

    Job job =
      Job.getInstance(conf, JOB_PREFIX + SelectAndAggregate.class.getName());
    Scan scan = new Scan();
    scan.setCaching(scanCache);
    scan.setCacheBlocks(false);

    // map
    TableMapReduceUtil.initTableMapperJob(verticesTableName,
      scan, SelectAndAggregate.SelectMapper.class, LongWritable.class,
      GenericPairWritable.class, job);

    // reduce
    TableMapReduceUtil.initTableReducerJob(graphsTableName,
      SelectAndAggregate.AggregateReducer.class, job);
    job.setNumReduceTasks(reducers);

    // run
    return job.waitForCompletion(verbose);
  }

  /**
   * Predicate to select graphs containing a vertex with an edge having the
   * label owl:sameAs .
   */
  public static class SameAsPredicate implements VertexPredicate {
    /*
     * TODO replace dummy.
     * vertex needs out/in edges with owl:sameAs label
     */

    @Override
    public boolean evaluate(Vertex vertex) {
      return true;
    }
  }

  /**
   * Aggregate function, not (yet) needed.
   */
  public static class ComponentAggregate implements VertexAggregate {

    @Override
    public Writable aggregate(Vertex vertex) {
      return new IntWritable(1);
    }
  }

  /**
   * Calculates the sum from a given set of values.
   */
  public static class ComponentCountAggregator implements PairAggregator {

    @Override
    public Pair<Boolean, ? extends Number> aggregate(
      Iterable<GenericPairWritable> values) {
      int count = 0;
      for (GenericPairWritable value : values) {
        count += ((IntWritable) value.getValue().get()).get();
      }
      return new Pair<>(true, count);
    }
  }

  /**
   * Runs the job.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new RDFAnalysisDriver(), args));
  }

  /**
   * Configuration params for
   * {@link org.gradoop.rdf.examples.RDFAnalysisDriver}.
   */
  public static class ConfUtils extends ConfigurationUtils {
    /**
     * Command line option to skip the import of data.
     */
    public static final String OPTION_SKIP_IMPORT = "si";
    /**
     * Command line option to skip the enrichment process of data.
     */
    public static final String OPTION_SKIP_ENRICHMENT = "se";

    static {
      OPTIONS.addOption(OPTION_SKIP_IMPORT, "skipImport", false,
        "Skip import of data.");
      OPTIONS.addOption(OPTION_SKIP_ENRICHMENT, "skipEnrichment", false,
        "Skip enrichment of data.");
    }
  }
}
