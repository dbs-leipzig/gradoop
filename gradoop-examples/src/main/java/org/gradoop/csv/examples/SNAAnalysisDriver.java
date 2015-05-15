package org.gradoop.csv.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.LabelPropagationComputation;
import org.gradoop.csv.io.reader.CSVReader;
import org.gradoop.drivers.BulkDriver;
import org.gradoop.io.formats.EPGLabelPropagationInputFormat;
import org.gradoop.io.formats.EPGLabelPropagationOutputFormat;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Runs the SNB Analysis Example
 */
public class SNAAnalysisDriver extends BulkDriver {

  private static final Logger LOG = Logger.getLogger(SNAAnalysisDriver.class);

  private static final String JOB_PREFIX = "SNB Analysis: ";

  static {
    Configuration.addDefaultResource("giraph-site.xml");
    Configuration.addDefaultResource("hbase-site.xml");
  }

  public SNAAnalysisDriver() {
    new LoadConfUtils();
  }

  /**
   * Starting point for SNB analysis pipeline
   *
   * @param args driver arguments
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


//    /*
//    Step 0: Delete (if exists) and create HBase tables
//     */
//    LOG.info("###DRIVER DropTables");
//    if (cmd.hasOption(LoadConfUtils.OPTION_DROP_TABLES)) {
//      HBaseGraphStoreFactory.deleteGraphStore(conf);
//    }
//    HBaseGraphStoreFactory.createOrOpenGraphStore(conf, new
// EPGVertexHandler(),
//      new EPGGraphHandler());
//
     /*
    Step 1: Bulk Load of the graph into HBase using MapReduce
     */
    if (cmd.hasOption(LoadConfUtils.OPTION_BULKLOAD)) {
      String inputPathHDFS = cmd.getOptionValue(OPTION_GRAPH_INPUT_PATH);
      String outputPathHDFS = cmd.getOptionValue(OPTION_GRAPH_OUTPUT_PATH);
      String metaDataPath =
        cmd.getOptionValue(LoadConfUtils.OPTION_METADATA_PATH);
      if (!runBulkLoad(conf, inputPathHDFS, outputPathHDFS, metaDataPath,
        verbose)) {
        return -1;
      }
    }

    /*
    Step 2: LabelPropagation Computation using Giraph
    */

    int workers =
      Integer.parseInt(cmd.getOptionValue(LoadConfUtils.OPTION_WORKERS));
    if (cmd.hasOption(LoadConfUtils.OPTION_LABLEPROPAGATION)) {
      if (!runLabelPropagationComputation(conf, workers, verbose)) {
        return -1;
      }
    }
    return 0;
  }


  /**
   * Runs the HFile conversion from the given file to the output dir. Also
   * loads the Hfiles to region servers.
   *
   * @param conf    Cluster config
   * @param inDir   input directory
   * @param outDir  HFile output dir in HDFS
   * @param verbose print output during job
   * @return true, if the job completed successfully, false otherwise
   * @throws Exception
   */
  private boolean runBulkLoad(Configuration conf, String inDir, String outDir,
    String metaDataPath, boolean verbose) throws Exception {
    Path inputDir = new Path(inDir);
    Path outputDir = new Path(outDir);

    String type;
    String label;
    String metaData;

    File[] csvFiles = new File(metaDataPath).listFiles();
    for (File file : csvFiles) {
      String fname = file.getName();
      if (fname.contains("_meta")) {
        BufferedReader br =
          new BufferedReader(new FileReader(metaDataPath + "/" + fname));
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
          } else{
            metaData = line;
          }
        }


        conf.set(CSVReader.TYPE, type);
        conf.set(CSVReader.LABEL, label);
        conf.set(CSVReader.META_DATA, metaData);
        conf.set(SNAAnalysisDriver.OPTION_GRAPH_INPUT_PATH,
          fname.replace("_meta", ""));

        // set line reader to read lines in input splits
        conf.setClass(BulkLoadEPG.VERTEX_LINE_READER, CSVReader.class,
          VertexLineReader.class);
        // set vertex handler that creates the Puts
        conf.setClass(BulkLoadEPG.VERTEX_HANDLER, EPGVertexHandler.class,
          VertexHandler.class);

        Job job =
          Job.getInstance(conf, JOB_PREFIX + BulkLoadEPG.class.getName());
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
        FileInputFormat.addInputPath(job, inputDir);
        // set output directory
        FileOutputFormat.setOutputPath(job, outputDir);

        HTable hTable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);

        // auto configure partitioner and reducer corresponding to the number of
        // regions
        HFileOutputFormat2.configureIncrementalLoad(job, hTable);

        // run job
        if (!job.waitForCompletion(verbose)) {
          LOG.error("###Error during bulk import ... stopping pipeline");
          return false;
        }

        // load created HFiles to the region servers
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(outputDir, hTable);


      }
    }


    return true;

  }

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

    conf.set("mapreduce.output.fileoutputformat.outputdir",
      "/user/hduser/output/sna");

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

    return job.run(true);
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

    public static final String OPTION_WORKERS = "w";

    public static final String OPTION_BULKLOAD = "bl";

    public static final String OPTION_LABLEPROPAGATION = "lp";

    public static final String OPTION_METADATA_PATH = "mdp";

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
