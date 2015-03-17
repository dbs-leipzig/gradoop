package org.gradoop.drivers;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.io.writer.BulkWriteEPG;
import org.gradoop.io.writer.VertexLineWriter;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Driver for {@link org.gradoop.io.writer.BulkWriteEPG}
 */
public class BulkWriteDriver extends Configured implements Tool {
  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(BulkWriteDriver.class);
  /**
   * Job name for map reduce job.
   */
  private static final String JOB_NAME = "Bulk Write Driver";

  static {
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    CommandLine cmd = ConfUtils.parseArgs(args);
    if (cmd == null) {
      return 0;
    }

    boolean verbose = cmd.hasOption(ConfUtils.OPTION_VERBOSE);
    String outputPath = cmd.getOptionValue(ConfUtils.OPTION_GRAPH_OUTPUT_PATH);
    String writerClassName =
      cmd.getOptionValue(ConfUtils.OPTION_VERTEX_LINE_WRITER);
    int hbaseScanCache = Integer
      .parseInt(cmd.getOptionValue(ConfUtils.OPTION_HBASE_SCAN_CACHE, "0"));
    if (cmd.hasOption(ConfUtils.OPTION_CUSTOM_ARGUMENT)) {
      for (String caOptionValue : cmd.getOptionValues("ca")) {
        for (String paramValue : Splitter.on(',').split(caOptionValue)) {
          String[] parts =
            Iterables.toArray(Splitter.on('=').split(paramValue), String.class);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Unable to parse custom " +
              " argument: " + paramValue);
          }
          //if (LOG.isInfoEnabled()) {
          LOG.info("###Setting custom argument [" + parts[0] + "] to [" +
            parts[1] + "] in GiraphConfiguration");
          //}
          conf.set(parts[0], parts[1]);
        }
      }
    }

    Class<? extends VertexLineWriter> writerClass =
      getLineWriterClass(writerClassName);

    if (!runBulkWrite(conf, writerClass, outputPath, hbaseScanCache, verbose)) {
      return -1;
    }
    return 0;
  }

  /**
   * Returns the class for the given class name parameter.
   *
   * @param writerClassName full qualified class name of the writer
   * @return writer class instance
   * @throws java.lang.ClassNotFoundException
   */
  private Class<? extends VertexLineWriter> getLineWriterClass(
    final String writerClassName) throws ClassNotFoundException {
    return Class.forName(writerClassName).asSubclass(VertexLineWriter.class);
  }

  /**
   * Setups and runs the job.
   *
   * @param conf           cluster config
   * @param writerClass    writer class for vertex conversion
   * @param outputDirName  directory to store output
   * @param hbaseScanCache hbase scan cache
   * @param verbose        print job output
   * @return true, iff the job succeeded
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private boolean runBulkWrite(final Configuration conf, final Class<?
    extends VertexLineWriter> writerClass, final String outputDirName,
    final int hbaseScanCache, final boolean verbose) throws IOException,
    ClassNotFoundException, InterruptedException {
    // setup job
    conf.setClass(BulkWriteEPG.VERTEX_LINE_WRITER, writerClass,
      VertexLineWriter.class);
    conf.setClass(BulkWriteEPG.VERTEX_HANDLER, EPGVertexHandler.class,
      VertexHandler.class);
    Job job = Job.getInstance(conf, JOB_NAME);
    job.setJarByClass(BulkWriteDriver.class);
    // setup scan to read from htable
    Scan scan = new Scan();
    if (hbaseScanCache > 0) {
      scan.setCaching(hbaseScanCache);
    }
    scan.setCacheBlocks(false);
    // setup map tasks
    TableMapReduceUtil
      .initTableMapperJob(GConstants.DEFAULT_TABLE_VERTICES, scan,
        BulkWriteEPG.class, Text.class, NullWritable.class, job);
    // no reduce needed for that job
    job.setNumReduceTasks(0);
    // set output path
    Path outputDir = new Path(outputDirName);
    FileOutputFormat.setOutputPath(job, outputDir);
    // run
    return job.waitForCompletion(verbose);
  }

  /**
   * Runs the job from console.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new BulkWriteDriver(), args));
  }

  /**
   * Configuration params for {@link org.gradoop.drivers.BulkWriteDriver}.
   */
  public static class ConfUtils {
    /**
     * Command line option for displaying help.
     */
    public static final String OPTION_HELP = "h";
    /**
     * Command line option for activating verbose.
     */
    public static final String OPTION_VERBOSE = "v";
    /**
     * Command line option for setting the vertex writer class.
     */
    public static final String OPTION_VERTEX_LINE_WRITER = "vlw";
    /**
     * Command line option for setting hbase scan cache.
     */
    public static final String OPTION_HBASE_SCAN_CACHE = "sc";
    /**
     * Command line option to set the path to write the graph to.
     */
    public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";
    /**
     * Command line option to set a custom argument.
     */
    public static final String OPTION_CUSTOM_ARGUMENT = "ca";
    /**
     * Holds options accepted by {@link org.gradoop.drivers.BulkWriteDriver}.
     */
    private static Options OPTIONS;

    static {
      OPTIONS = new Options();
      OPTIONS.addOption(OPTION_HELP, "help", false, "Display help.");
      OPTIONS.addOption(OPTION_VERBOSE, "verbose", false,
        "Print console output during job execution.");
      OPTIONS.addOption(OPTION_HBASE_SCAN_CACHE, "scan-cache", true,
        "Number of rows to read from HTable as input for map tasks.");
      OPTIONS.addOption(OPTION_VERTEX_LINE_WRITER, "vertex-line-writer", true,
        "VertexLineWriter implementation which is used to write a vertex to a" +
          " single line in the output.");
      OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graph-output-path", true,
        "Path where the output is stored.");
      OPTIONS.addOption(OPTION_CUSTOM_ARGUMENT, "customArguments", true,
        "provide custom" +
          " arguments for the job configuration in the form:" +
          " -ca <param1>=<value1>,<param2>=<value2> -ca <param3>=<value3> etc" +
          "." +
          " It can appear multiple times, and the last one has effect" +
          " for the same param.");
    }

    /**
     * Parses the given arguments.
     *
     * @param args command line arguments
     * @return parsed command line
     * @throws org.apache.commons.cli.ParseException
     */
    public static CommandLine parseArgs(final String[] args) throws
      ParseException {
      if (args.length == 0) {
        LOG.error("No arguments were provided (try -h)");
      }
      CommandLineParser parser = new BasicParser();
      CommandLine cmd = parser.parse(OPTIONS, args);

      if (cmd.hasOption(OPTION_HELP)) {
        printHelp();
        return null;
      }
      boolean sane = performSanityCheck(cmd);

      return sane ? cmd : null;
    }

    /**
     * Prints a help menu for the defined options.
     */
    private static void printHelp() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ConfUtils.class.getName(), OPTIONS, true);
    }

    /**
     * Checks if the given arguments are valid.
     *
     * @param cmd command line
     * @return true, iff the input is sane
     */
    private static boolean performSanityCheck(final CommandLine cmd) {
      boolean sane = true;
      if (!cmd.hasOption(OPTION_GRAPH_OUTPUT_PATH)) {
        LOG.error("Choose the graph output path (-gop)");
        sane = false;
      }
      if (!cmd.hasOption(OPTION_VERTEX_LINE_WRITER)) {
        LOG.error("Choose the vertex line writer. (-vlw)");
        sane = false;
      }
      return sane;
    }
  }
}
