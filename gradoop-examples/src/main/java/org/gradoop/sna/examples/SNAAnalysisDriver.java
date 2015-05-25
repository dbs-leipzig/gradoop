package org.gradoop.sna.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.LabelPropagationComputation;
import org.gradoop.algorithms.Summarize;
import org.gradoop.drivers.BulkDriver;
import org.gradoop.io.formats.EPGLabelPropagationInputFormat;
import org.gradoop.io.formats.EPGLabelPropagationOutputFormat;
import org.gradoop.io.formats.SummarizeWritable;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.sna.io.reader.CSVReader;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Runs the SNB Analysis Example
 */
public class SNAAnalysisDriver extends BulkDriver {
  /**
   * Class Logger
   */
  private static final Logger LOG = Logger.getLogger(SNAAnalysisDriver.class);
  /**
   * Job Prefix
   */
  private static final String JOB_PREFIX = "SNB Analysis: ";

  /**
   * static block
   */
  static {
    Configuration.addDefaultResource("giraph-site.xml");
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * Constructor
   */
  public SNAAnalysisDriver() {
    new LoadConfUtils();
  }

  /**
   * Starting point for SNB analysis pipeline
   *
   * @param args driver arguments
   *             cmd.getOptionValue(LoadConfUtils.OPTION_METADATA_PATH);
   *             String type;
   *             String label;
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    int check = parseArgs(args);
    if (check == 0) {
      return 0;
    }
    CommandLine cmd = LoadConfUtils.parseArgs(args);
    if (cmd == null) {
      return 0;
    }
    boolean verbose = cmd.hasOption(OPTION_VERBOSE);
    Configuration conf = getHadoopConf();

    /*
    Step 0: Drop Tables if needed
     */
    if (cmd.hasOption(LoadConfUtils.OPTION_DROP_TABLES)) {
      HBaseGraphStoreFactory
        .createOrOpenGraphStore(conf, new EPGVertexHandler(),
          new EPGGraphHandler());
      HBaseGraphStoreFactory.deleteGraphStore(conf);
    }
    // Create or Open GraphStore
    HBaseGraphStoreFactory.createOrOpenGraphStore(conf, new EPGVertexHandler(),
      new EPGGraphHandler());
     /*
    Step 1: Bulk Load of the graph into HBase using MapReduce
     */
    if (cmd.hasOption(LoadConfUtils.OPTION_BULKLOAD)) {
      String separator = System.getProperty("file.separator");
      String metaDataPath =
        cmd.getOptionValue(LoadConfUtils.OPTION_METADATA_PATH);
      String type;
      String label;
      String metaData;
      File[] csvFiles = new File(metaDataPath).listFiles();
      assert csvFiles != null;
      for (File file : csvFiles) {
        String fname = file.getName();
        if (fname.contains("_meta")) {
          BufferedReader br = null;
          try {
            br = new BufferedReader(new InputStreamReader(
              new FileInputStream(metaDataPath + separator + fname), "UTF8"));
            String line;
            type = "";
            label = "";
            metaData = "";
            int lineNr = 1;
            while ((line = br.readLine()) != null) {
              if (lineNr == 1) {
                type = line;
                lineNr++;
              } else if (lineNr == 2) {
                label = line;
                lineNr++;
              } else {
                metaData = line;
              }
            }
            conf.set(CSVReader.TYPE, type);
            conf.set(CSVReader.LABEL, label);
            conf.set(CSVReader.META_DATA, metaData);
            String hdfsInputPath = cmd.getOptionValue(OPTION_GRAPH_INPUT_PATH);
            String hdfsFileName = fname.replace("_meta", "");
            String hdfsinputFilePath = hdfsInputPath + hdfsFileName;
            String outputPathHDFS =
              cmd.getOptionValue(OPTION_GRAPH_OUTPUT_PATH) +
                separator +
                hdfsinputFilePath;
            LOG.info("Run Bulkload: " + hdfsFileName);
            if (!runBulkLoad(conf, hdfsinputFilePath, outputPathHDFS,
              verbose)) {
              return -1;
            }
          } catch (IOException e) {
            System.err.println("IOExcepton: " + e.getMessage());
          } finally {
            if (br != null) {
              br.close();
            }
          }
        }
      }
    }

    /*
    Step 2: LabelPropagation Computation using Giraph
    */
    if (cmd.hasOption(LoadConfUtils.OPTION_LABLEPROPAGATION)) {
      int workers =
        Integer.parseInt(cmd.getOptionValue(LoadConfUtils.OPTION_WORKERS));
      LOG.info("Run Label-Propagation");
      if (!runLabelPropagationComputation(conf, workers, verbose)) {
        LOG.info("####LP Failure");
        return -1;
      }
    }

    /*
    Step 3: Summarization
     */
    if (cmd.hasOption(LoadConfUtils.OPTION_SUMMARIZE)) {
      String path =
        cmd.getOptionValue(LoadConfUtils.OPTION_SUMMARIZE_OUTPUT_PATH);
      int reduce = Integer
        .parseInt(cmd.getOptionValue(LoadConfUtils.OPTION_REDUCERS, "1"));
      int scanCache = Integer
        .parseInt(cmd.getOptionValue(LoadConfUtils.OPTION_SCAN_CACHE, "500"));
      if (!runSummarize(conf, scanCache, reduce, verbose, path)) {
        LOG.info("####MP Failure");
        return -1;
      }
    }
    return 0;
  }

  /**
   * Runs the HFile conversion from the given file to the output dir. Also
   * loads the Hfiles to region servers.
   *
   * @param conf      Cluster config
   * @param graphFile graph file in hdfs
   * @param outDir    HFile output dir in HDFS
   * @param verbose   print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws Exception
   */
  private boolean runBulkLoad(Configuration conf, String graphFile,
    String outDir, boolean verbose) throws Exception {
    Path inputFile = new Path(graphFile);
    Path outputDir = new Path(outDir);
    // set line reader to read lines in input splits
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER, CSVReader.class,
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
   * runs Label Propagation Computation
   *
   * @param conf        hadoop conf
   * @param workerCount worker for giraph computation
   * @param verbose     verbose
   * @return true, if the job completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws ParseException
   */
  private boolean runLabelPropagationComputation(Configuration conf,
    int workerCount, boolean verbose) throws IOException,
    ClassNotFoundException, InterruptedException, ParseException {
    // set HBase table to read graph from
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    // just scan necessary CFs (no properties needed)
    String columnFamiliesToScan = String
      .format("%s %s %s %s", GConstants.CF_LABELS, GConstants.CF_OUT_EDGES,
        GConstants.CF_IN_EDGES, GConstants.CF_GRAPHS);
    conf.set(TableInputFormat.SCAN_COLUMNS, columnFamiliesToScan);
    // set HBase table to write computation results to
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    /**
     * Configuration for giraph output to fs
     */
    conf.set("mapreduce.output.fileoutputformat.outputdir", "output/lp");
    // setup Giraph job
    GiraphJob job = new GiraphJob(conf,
      JOB_PREFIX + LabelPropagationComputation.class.getName());
    GiraphConfiguration giraphConf = job.getConfiguration();
    giraphConf.setComputationClass(LabelPropagationComputation.class);
    giraphConf.setVertexInputFormatClass(EPGLabelPropagationInputFormat.class);
    //giraph output or hbaseoutput
    giraphConf
      .setVertexOutputFormatClass(EPGLabelPropagationOutputFormat.class);
    giraphConf.setWorkerConfiguration(workerCount, workerCount, 100f);
    // assuming local environment
    if (workerCount == 1) {
      GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf, false);
      GiraphConstants.LOCAL_TEST_MODE.set(giraphConf, true);
    }
    return job.run(verbose);
  }

  /**
   * runs Summarize MapReduce job
   *
   * @param conf      hadoop conf
   * @param scanCache scan cache size
   * @param reducers  reducer class
   * @param verbose   verbose
   * @param path      output path
   * @return true, if the job completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private boolean runSummarize(Configuration conf, int scanCache, int reducers,
    boolean verbose, String path) throws IOException, ClassNotFoundException,
    InterruptedException {
    Path outputPath = new Path(path);

    /*
     mapper settings
      */
    // vertex handler
    conf.setClass(GConstants.VERTEX_HANDLER_CLASS, EPGVertexHandler.class,
      VertexHandler.class);
    /*
    reducer settings
     */
    // graph handler
    conf.setClass(GConstants.GRAPH_HANDLER_CLASS, EPGGraphHandler.class,
      GraphHandler.class);
    Job job = Job.getInstance(conf, JOB_PREFIX + Summarize.class.getName());
    Scan scan = new Scan();
    scan.setCaching(scanCache);
    scan.setCacheBlocks(false);
    // map
    TableMapReduceUtil
      .initTableMapperJob(GConstants.DEFAULT_TABLE_VERTICES, scan,
        Summarize.SummarizeMapper.class, LongWritable.class,
        SummarizeWritable.class, job);
    // reduce
    job.setReducerClass(Summarize.SummarizeReducer.class);
    job.setNumReduceTasks(reducers);
    job.setOutputFormatClass(TextOutputFormat.class);
    conf.set(TextOutputFormat.SEPERATOR, " ");
    FileOutputFormat.setOutputPath(job, outputPath);
    // run
    return job.waitForCompletion(verbose);
  }

  /**
   * OPTION_BULKLOAD
   * Configuration params for {@link org.gradoop.drivers.BulkLoadDriver}.
   */
  public static class LoadConfUtils extends ConfUtils {
    /**
     * Command line option for setting the vertex reader class.
     */
    public static final String OPTION_VERTEX_LINE_READER = "vlr";
    /**
     * Command line option to drop hbase tables if they exist.
     */
    public static final String OPTION_DROP_TABLES = "dt";
    /**
     * Command line option for setting giraph worker
     */
    public static final String OPTION_WORKERS = "w";
    /**
     * Command line option for starting a new bulk load
     */
    public static final String OPTION_BULKLOAD = "bl";
    /**
     * Command line option for starting the label propagation computation
     */
    public static final String OPTION_LABLEPROPAGATION = "lp";
    /**
     * Command line option for setting the meta data path
     */
    public static final String OPTION_METADATA_PATH = "mdp";
    /**
     * Command line option for setting giraph output path (unused atm)
     * Todo: using mapred.output.path
     */
    public static final String OPTION_GIRAPH_OUTPUT_PATH = "gop";
    /**
     * Command line option for starting summarize mapreduce job
     */
    public static final String OPTION_SUMMARIZE = "sum";
    /**
     * Command line option for setting the reducer count
     */
    public static final String OPTION_REDUCERS = "rs";
    /**
     * Command line option for setting hbase scan cache
     */
    public static final String OPTION_SCAN_CACHE = "sc";
    /**
     * Command line option for setting summarize output path
     */
    public static final String OPTION_SUMMARIZE_OUTPUT_PATH = "sop";

    static {
      OPTIONS.addOption(OPTION_DROP_TABLES, "drop-tables", false,
        "Drop HBase EPG tables if they exist.");
      OPTIONS.addOption(OPTION_VERTEX_LINE_READER, "vertex-line-reader", true,
        "VertexLineReader implerementation which is used to read a single " +
          "line " +
          "in the input.");
      OPTIONS
        .addOption(OPTION_WORKERS, "workers", true, "Number of giraph workers");
      OPTIONS.addOption(OPTION_BULKLOAD, "bulkload", false,
        "Starts new " + "Bulkload");
      OPTIONS.addOption(OPTION_LABLEPROPAGATION, "labelpropagation", false,
        "Starts LabelPropagation");
      OPTIONS.addOption(OPTION_METADATA_PATH, "metadatapath", true,
        "Path to " + "CSV MetaData");
      OPTIONS.addOption(OPTION_GIRAPH_OUTPUT_PATH, "giraphoutputpath", true,
        "Path to Giraph - Output");
      OPTIONS.addOption(OPTION_SUMMARIZE, "summarize", false,
        "Add the " + "summarize option to the anlysis pipeline");
      OPTIONS.addOption(OPTION_REDUCERS, "reducers", true,
        "Number of " + "Reducers");
      OPTIONS.addOption(OPTION_SCAN_CACHE, "scancache", true,
        "Mapreduce " + "scan-cahce");
      OPTIONS
        .addOption(OPTION_SUMMARIZE_OUTPUT_PATH, "summarizeoutputpath", true,
          "Summarize outout Path");
    }
  }

  /**
   * Runs the job.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.exit(ToolRunner.run(conf, new SNAAnalysisDriver(), args));
  }
}
