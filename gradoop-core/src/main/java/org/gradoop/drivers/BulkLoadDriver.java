package org.gradoop.drivers;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

/**
 * Driver program for running a bulk load.
 */
public class BulkLoadDriver extends BulkDriver implements Tool {
  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(BulkLoadDriver.class);
  /**
   * Job name for map reduce job.
   */
  private static final String JOB_NAME = "Bulk Load Driver";

  static {
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int run(String[] args) throws Exception {
    LOG.info("####parseArgs");
    LoadConfUtils.setOptions();
    parseArgs(args);
    LOG.info("####inputPath: " + inputPath);
    LOG.info("####outputPath: " + outputPath);


    CommandLine cmd = LoadConfUtils.parseArgs(args);
    if (cmd == null) {
      return 0;
    }
    boolean sane = performSanityCheck(cmd);
    if (sane) {
      return -1;
    }
    boolean dropTables = cmd.hasOption(LoadConfUtils.OPTION_DROP_TABLES);
    String readerClassName =
      cmd.getOptionValue(LoadConfUtils.OPTION_VERTEX_LINE_READER);
    Class<? extends VertexLineReader> readerClass =
      getLineReaderClass(readerClassName);
    createGraphStore(conf, dropTables);
    if (!runBulkLoad(conf, readerClass, inputPath, outputPath, verbose)) {
      return -1;
    }
    return 0;
  }

  /**
   * Returns the class for the given class name parameter.
   *
   * @param readerClassName full qualified class name of the reader
   * @return reader class instance
   * @throws java.lang.ClassNotFoundException
   */
  private Class<? extends VertexLineReader> getLineReaderClass(
    final String readerClassName) throws ClassNotFoundException {
    LOG.info("###getLineReader: " + Class.forName(readerClassName).asSubclass
      (VertexLineReader.class));
    return Class.forName(readerClassName).asSubclass(VertexLineReader.class);
  }

  /**
   * Opens an existing or creates a new graph store.
   *
   * @param conf       cluster config
   * @param dropTables true, if existing tables shall be dropped
   */
  private void createGraphStore(final Configuration conf, boolean dropTables) {
    if (dropTables) {
      HBaseGraphStoreFactory.deleteGraphStore(conf);
    }
    HBaseGraphStoreFactory.createOrOpenGraphStore(conf, new EPGVertexHandler(),
      new EPGGraphHandler());
  }

  /**
   * Checks if the given arguments are valid.
   *
   * @param cmd command line
   * @return true, iff the input is sane
   */
  private static boolean performSanityCheck(final CommandLine cmd) {
    boolean sane = true;
    if (!cmd.hasOption(LoadConfUtils.OPTION_VERTEX_LINE_READER)) {
      LOG.error("Choose the vertex line reader (-vlr)");
      sane = false;
    }
    return sane;
  }

  /**
   * Run the actual bulk load job.
   *
   * @param conf          cluster config
   * @param readerClass   class for reading input lines
   * @param graphFileName input file
   * @param outputDirName hfile output dir
   * @param verbose       print job output
   * @return true, iff the job succeeded
   * @throws Exception
   */
  private boolean runBulkLoad(final Configuration conf,
    final Class<? extends VertexLineReader> readerClass,
    final String graphFileName, final String outputDirName,
    final boolean verbose) throws Exception {
    Path inputFile = new Path(graphFileName);
    Path outputDir = new Path(outputDirName);
    // set line reader to read lines in input splits
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER, readerClass,
      VertexLineReader.class);
    // set vertex handler that creates the hbase Puts
    conf.setClass(BulkLoadEPG.VERTEX_HANDLER, EPGVertexHandler.class,
      VertexHandler.class);
    // create job
    Job job = Job.getInstance(conf, JOB_NAME);
    job.setJarByClass(BulkLoadDriver.class);
    // mapper the runs the HFile conversion
    job.setMapperClass(BulkLoadEPG.class);
    // input format for mapper (File)
    job.setInputFormatClass(TextInputFormat.class);
    // output key class of Mapper
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    // output value class of Mapper
    job.setMapOutputValueClass(Put.class);
    // set input file
    FileInputFormat.addInputPath(job, inputFile);
    // set output directory
    FileOutputFormat.setOutputPath(job, outputDir);
    HTable htable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);
    // auto configure partitioner and reducer based on table settings (e.g.
    // number of regions)
    HFileOutputFormat2.configureIncrementalLoad(job, htable);
    // run job
    if (!job.waitForCompletion(verbose)) {
      LOG.error("Error during hfile creation, stopping job.");
      return false;
    }
    // load created HFiles to the region servers
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, htable);
    return true;
  }

  /**
   * Runs the job from console.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new BulkLoadDriver(), args));
  }

  public static class LoadConfUtils extends ConfUtils {
    /**
     * Command lne option for setting the vertex reader class.
     */
    public static final String OPTION_VERTEX_LINE_READER = "vlr";
    /**
     * Command line option to drop hbase tables if they exist.
     */
    public static final String OPTION_DROP_TABLES = "dt";

    static {
      OPTIONS.addOption(OPTION_DROP_TABLES, "drop-tables", false,
        "Drop HBase EPG tables if they exist.");
      OPTIONS.addOption(OPTION_VERTEX_LINE_READER, "vertex-line-reader", true,
        "VertexLineReader implementation which is used to read a single line " +
          "in the input.");
      LOG.info("###options added drop table and vertex line reader");
    }

    public static void setOptions(){
      OPTIONS.addOption(OPTION_DROP_TABLES, "drop-tables", false,
        "Drop HBase EPG tables if they exist.");
      OPTIONS.addOption(OPTION_VERTEX_LINE_READER, "vertex-line-reader", true,
        "VertexLineReader implementation which is used to read a single line " +
          "in the input.");
      LOG.info("###options added drop table and vertex line reader");
    }


  }
}
