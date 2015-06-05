package org.gradoop.rdf.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.ConnectedComponentsComputation;
import org.gradoop.algorithms.PairAggregator;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.io.formats.EPGHBaseVertexInputFormat;
import org.gradoop.io.formats.GenericPairWritable;
import org.gradoop.io.formats.SubgraphExtractionVertexOutputFormat;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Vertex;
import org.gradoop.model.operators.VertexAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

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
   * Starting point for BTG analysis pipeline.
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    CommandLine cmd = ConfigurationUtils.parseArgs(args);

    if (cmd == null) {
      return 0;
    }

    boolean verbose = cmd.hasOption(ConfigurationUtils.OPTION_VERBOSE);

    /*
    Step 0: Delete (if exists) and create HBase tables
     */
    if (cmd.hasOption(ConfigurationUtils.OPTION_DROP_TABLES)) {
      HBaseGraphStoreFactory.deleteGraphStore(conf);
    }
    HBaseGraphStoreFactory.createOrOpenGraphStore(conf, new EPGVertexHandler(),
      new EPGGraphHandler());

    /*
    Step 1: Bulk Load of the graph into HBase using MapReduce
     */
    String inputPath =
      cmd.getOptionValue(ConfigurationUtils.OPTION_GRAPH_INPUT_PATH);
    String outputPath =
      cmd.getOptionValue(ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH);
    if (!runBulkLoad(conf, inputPath, outputPath, verbose)) {
      return -1;
    }

    /*
    Step 2: CC Computation using Giraph
     */
    int workers =
      Integer.parseInt(cmd.getOptionValue(ConfigurationUtils.OPTION_WORKERS));
    if (!runRDFComputation(conf, workers, verbose)) {
      return -1;
    }

    /*
    Step 3: Select And Aggregate using MapReduce
     */
    int scanCache = Integer.parseInt(
      cmd.getOptionValue(ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500"));
    int reducers = Integer
      .parseInt(cmd.getOptionValue(ConfigurationUtils.OPTION_REDUCERS, "1"));
    if (!runSelectAndAggregate(conf, scanCache, reducers, verbose)) {
      return -1;
    }

    return 0;
  }

  /**
   * Runs the HFile conversion from the given file to the output dir. Also
   * loads the HFiles to region servers.
   *
   * @param conf      Cluster config
   * @param graphFile input file in HDFS
   * @param outDir    HFile output dir in HDFS
   * @param verbose   print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws java.io.IOException
   */
  private boolean runBulkLoad(Configuration conf, String graphFile,
    String outDir, boolean verbose) throws Exception {
    Path inputFile = new Path(graphFile);
    Path outputDir = new Path(outDir);

    // set line reader to read lines in input splits
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER, RDFReader.class,
      VertexLineReader.class);
    // set vertex handler that creates the Puts
    conf.setClass(BulkLoadEPG.VERTEX_HANDLER, EPGVertexHandler.class,
      VertexHandler.class);

    Job job = Job.getInstance(conf, JOB_PREFIX + BulkLoadEPG.class.getName());
    job.setJarByClass(BulkLoadEPG.class);
    // mapper that runs the HFile conversion
    job.setMapperClass(BulkLoadEPG.class);
    // input format for Mapper (File)
    job.setInputFormatClass(TextInputFormat.class);
    // output Key class of Mapper
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    // output Value class of Mapper
    job.setMapOutputValueClass(Put.class);

    // set input file
    FileInputFormat.addInputPath(job, inputFile);
    // set output directory
    FileOutputFormat.setOutputPath(job, outputDir);

    HTable hTable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);

    // auto configure partitioner and reducer corresponding to the number of
    // regions
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

    // run job
    if (!job.waitForCompletion(verbose)) {
      LOG.error("Error during bulk import ... stopping pipeline");
      return false;
    }

    // load created HFiles to the region servers
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, hTable);

    return true;
  }

  /**
   * Runs the computation on the input graph using Giraph.
   *
   * @param conf        Cluster configuration
   * @param workerCount Number of workers Giraph shall use
   * @param verbose     print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws org.apache.commons.cli.ParseException
   */
  private boolean runRDFComputation(Configuration conf, int workerCount,
    boolean verbose) throws IOException, ClassNotFoundException,
    InterruptedException, ParseException {
    // set HBase table to read graph from
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    // just scan necessary CFs (no properties needed)
    String columnFamiliesToScan = String
      .format("%s %s %s %s", GConstants.CF_META, GConstants.CF_OUT_EDGES,
        GConstants.CF_IN_EDGES, GConstants.CF_GRAPHS);
    conf.set(TableInputFormat.SCAN_COLUMNS, columnFamiliesToScan);
    // set HBase table to write computation results to
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);

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
   * @param verbose   print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private boolean runSelectAndAggregate(Configuration conf, int scanCache,
    int reducers, boolean verbose) throws IOException, ClassNotFoundException,
    InterruptedException {
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
    TableMapReduceUtil.initTableMapperJob(GConstants.DEFAULT_TABLE_VERTICES,
      scan, SelectAndAggregate.SelectMapper.class, LongWritable.class,
      GenericPairWritable.class, job);

    // reduce
    TableMapReduceUtil.initTableReducerJob(GConstants.DEFAULT_TABLE_GRAPHS,
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
}
