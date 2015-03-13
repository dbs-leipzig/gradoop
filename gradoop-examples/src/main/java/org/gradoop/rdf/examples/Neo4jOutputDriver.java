package org.gradoop.rdf.examples;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;
import org.gradoop.io.writer.Neo4jLineWriter;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Read vertices and edges from Gradoop and write them to Neo4j embedded db.
 */
public class Neo4jOutputDriver extends Configured {
  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(Neo4jOutputDriver.class);

  /**
   * Output path for Neo4j embedded db.
   */
  private String outputPath;

  /**
   * GraphStore
   */
  private GraphStore graphStore;

  /**
   * Neo4jOutputDriver constructor setting up configuration values.
   * @param args cmd line parameters
   * @throws Exception
   */
  public Neo4jOutputDriver(String[] args) throws Exception {
    CommandLine cmd = ConfUtils.parseArgs(args);
    this.outputPath = cmd.getOptionValue(ConfUtils.OPTION_NEO4J_OUTPUT_PATH);
  }

  /**
   * Write output to the defined Neo4j instance.
   * @throws Exception
   */
  public void writeOutput() throws Exception {
    Neo4jLineWriter writer = new Neo4jLineWriter(outputPath);
    GraphDatabaseService db = writer.getGraphDbService();

    try (Transaction tx = db.beginTx()) {
      for (Vertex vertex : graphStore.readVertices()) {
        writer.writeVertex(vertex);
      }
      tx.success();
    }

    try (Transaction tx = db.beginTx()) {
      for (Vertex vertex : graphStore.readVertices()) {
        writer.writeEdges(vertex);
      }
      tx.success();
    }

    writer.shutdown();
    graphStore.close();
  }

  /**
   * Runs the job from console.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Neo4jOutputDriver driver = new Neo4jOutputDriver(args);
    driver.writeOutput();
  }

  public void setGraphStore(GraphStore graphStore) {
    this.graphStore = graphStore;
  }

  /**
   * Config params for {@link org.gradoop.rdf.examples.Neo4jOutputDriver}.
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
     * Command line option to set the path to write the graph to.
     */
    public static final String OPTION_NEO4J_OUTPUT_PATH = "out";
    /**
     * Holds options accepted by {@link org.gradoop.drivers.BulkWriteDriver}.
     */
    private static Options OPTIONS;

    static {
      OPTIONS = new Options();
      OPTIONS.addOption(OPTION_HELP, "help", false, "Display help.");
      OPTIONS.addOption(OPTION_VERBOSE, "verbose", false,
        "Print console output during job execution.");
      OPTIONS.addOption(OPTION_NEO4J_OUTPUT_PATH, "neo4j-output-path", true,
        "Path where the output will be stored.");
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
      if (!cmd.hasOption(OPTION_NEO4J_OUTPUT_PATH)) {
        LOG.error("Choose the neo4j output path (-out)");
        sane = false;
      }
      return sane;
    }
  }
}
