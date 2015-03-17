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
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * BulkDriver SuperClass for all Driver
 */
public abstract class BulkDriver extends Configured implements Tool {
  /**
   * Class Logger
   */
  private static Logger LOG = Logger.getLogger(BulkDriver.class);
  /**
   * Command line option for displaying help.
   */
  public static final String OPTION_HELP = "h";
  /**
   * Command line option for activating verbose.
   */
  public static final String OPTION_VERBOSE = "v";
  /**
   * Command line option to set the graph file input path.
   */
  public static final String OPTION_GRAPH_INPUT_PATH = "gip";
  /**
   * Command line option to set the path to store HFiles from Bulk Load.
   */
  public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";
  /**
   * Command line option to set a custom argument.
   */
  public static final String OPTION_CUSTOM_ARGUMENT = "ca";
  public boolean verbose;
  public String inputPath;
  public String outputPath;
  public Configuration conf;

  public void parseArgs(String[] args) throws ParseException {
    conf = getConf();
    CommandLine cmd = ConfUtils.parseArgs(args);
    verbose = cmd.hasOption(OPTION_VERBOSE);
    inputPath = cmd.getOptionValue(OPTION_GRAPH_INPUT_PATH);
    outputPath = cmd.getOptionValue(OPTION_GRAPH_OUTPUT_PATH);
    if (cmd.hasOption(OPTION_CUSTOM_ARGUMENT)) {
      for (String caOptionValue : cmd.getOptionValues(OPTION_CUSTOM_ARGUMENT)) {
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
  }

  /**
   * Configuration params for {@link org.gradoop.drivers.BulkLoadDriver}.
   */
  public static class ConfUtils {
    /**
     * Maintains accepted options for {@link org.gradoop.drivers.BulkLoadDriver}
     */
    protected static Options OPTIONS;

    static {
      OPTIONS = new Options();
      OPTIONS.addOption(OPTION_HELP, "help", false, "Display help.");
      OPTIONS.addOption(OPTION_VERBOSE, "verbose", false,
        "Print console output during job execution.");
      OPTIONS.addOption(OPTION_GRAPH_INPUT_PATH, "graph-input-path", true,
        "Path to the input graph file.");
      OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graph-output-path", true,
        "Path to store HFiles in HDFS (used by Bulk Load)");
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
      CommandLine cmd = parser.parse(OPTIONS, args, true);
      if (cmd.hasOption(OPTION_HELP)) {
        printHelp();
        return null;
      }
      boolean sane = performSanityCheck(cmd);

      return sane ? cmd : null;
    }

    private static boolean performSanityCheck(final CommandLine cmd){
      boolean sane = true;
      if (!cmd.hasOption(OPTION_GRAPH_OUTPUT_PATH)) {
        LOG.error("Choose the graph output path (-gop)");
        sane = false;
      }
      if (!cmd.hasOption(OPTION_GRAPH_INPUT_PATH)) {
        LOG.error("Choose the graph input path. (-gip)");
        sane = false;
      }
      return sane;
    }

    /**
     * Prints a help menu for the defined options.
     */
    private static void printHelp() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ConfUtils.class.getName(), OPTIONS, true);
    }
  }
}
