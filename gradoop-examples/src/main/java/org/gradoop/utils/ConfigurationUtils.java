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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.utils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Translate command line args into Configuration Key-Value pairs.
 */
public class ConfigurationUtils {
  /**
   * Command line option for displaying help.
   */
  public static final String OPTION_HELP = "h";
  /**
   * Command line option for activating verbose.
   */
  public static final String OPTION_VERBOSE = "v";
  /**
   * Command line option to set the number of giraph workers.
   */
  public static final String OPTION_WORKERS = "w";
  /**
   * Command line option to set the number of reducers.
   */
  public static final String OPTION_REDUCERS = "r";
  /**
   * Command line option to set the hbase scan cache value.
   */
  public static final String OPTION_HBASE_SCAN_CACHE = "sc";
  /**
   * Command line option to drop hbase tables if they exist.
   */
  public static final String OPTION_DROP_TABLES = "dt";
  /**
   * Command line option to set the graph file input path.
   */
  public static final String OPTION_GRAPH_INPUT_PATH = "gip";
  /**
   * Command line option to set the path to store HFiles from Bulk Load.
   */
  public static final String OPTION_GRAPH_OUTPUT_PATH = "gop";
  /**
   * Create custom vertices table for different use cases.
   */
  public static final String OPTION_TABLE_PREFIX = "tp";
  /**
   * Command line option to skip the import of data.
   */
  public static final String OPTION_SKIP_IMPORT = "si";
  /**
   * Command line option to skip the enrichment process of data.
   */
  public static final String OPTION_SKIP_ENRICHMENT = "se";
  /**
   * Command line option to set the table name for the secondary index.
   */
  public static final String OPTION_SORT_TABLE_NAME = "stn";

  /**
   * Maintains accepted options for gradoop-examples
   */
  protected static final Options OPTIONS;

  static {
    OPTIONS = new Options();
    OPTIONS.addOption(OPTION_HELP, "help", false, "Help");
    OPTIONS.addOption(OPTION_VERBOSE, "verbose", false, "Print output");
    OPTIONS
      .addOption(OPTION_WORKERS, "workers", true, "Number of giraph workers");
    OPTIONS.addOption(OPTION_REDUCERS, "reducers", true, "Number of reducers");
    OPTIONS.addOption(OPTION_HBASE_SCAN_CACHE, "scanCache", true,
      "HBase scan" + " cache");
    OPTIONS.addOption(OPTION_DROP_TABLES, "dropTables", false,
      "Drop HBase EPG tables if they exist.");
    OPTIONS.addOption(OPTION_GRAPH_INPUT_PATH, "graphInputPath", true,
      "Graph Input Path");
    OPTIONS.addOption(OPTION_GRAPH_OUTPUT_PATH, "graphOutputPath", true,
      "HFiles output path (used by Bulk Load)");
    OPTIONS.addOption(OPTION_TABLE_PREFIX, "table-prefix",
      true, "Custom prefix for HBase table to distinguish different use " +
        "cases.");
    OPTIONS.addOption(OPTION_SORT_TABLE_NAME, "sortTableName", true, "HTable " +
      "used to store secondary index.");
  }

  /**
   * Parses the given arguments.
   *
   * @param args command line arguments
   * @return parsed command line
   * @throws ParseException
   */
  public static CommandLine parseArgs(final String[] args) throws
    ParseException {
    if (args.length == 0) {
      throw new IllegalArgumentException("No arguments were provided (try -h)");
    }
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    if (cmd.hasOption(OPTION_HELP)) {
      printHelp();
      return null;
    }
    performSanityCheck(cmd);

    return cmd;
  }

  /**
   * Prints a help menu for the defined options.
   */
  protected static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(ConfigurationUtils.class.getName(), OPTIONS, true);
  }

  /**
   * Checks if the given arguments make sense.
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_WORKERS)) {
      throw new IllegalArgumentException("Chose the number of workers (-w)");
    }
    if (!cmd.hasOption(OPTION_GRAPH_INPUT_PATH) &&
      !cmd.hasOption(OPTION_SKIP_IMPORT)) {
      throw new IllegalArgumentException("Chose the graph input path (-gip");
    }
    if (!cmd.hasOption(OPTION_GRAPH_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Chose the graph output path (-gop");
    }
  }
}
