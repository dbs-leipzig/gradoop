package org.gradoop.rdf.examples;

import com.google.common.collect.Lists;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.io.writer.Neo4jLineWriter;
import org.gradoop.model.Graph;

import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Read vertices and edges from Gradoop and write them to Neo4j embedded db.
 */
public class Neo4jOutputDriver extends Configured implements Tool {
  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(Neo4jOutputDriver.class);

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    CommandLine cmd = ConfUtils.parseArgs(args);
    if (cmd == null) {
      return 0;
    }

    String tablePrefix = cmd.getOptionValue(ConfUtils.OPTION_TABLE_PREFIX, "");
    String outDir = cmd.getOptionValue(ConfUtils.OPTION_NEO4J_OUTPUT_PATH, "");

    // Open HBase tables
    GraphStore graphStore = HBaseGraphStoreFactory.createOrOpenGraphStore(conf,
      new EPGVertexHandler(), new EPGGraphHandler(), tablePrefix);

    writeOutput(graphStore, outDir, tablePrefix);
    graphStore.close();

    return 0;
  }

  /**
   * Write output to the defined Neo4j instance.
   * @throws Exception
   * @param graphStore gs
   * @param outputDir out directory
   * @param tablePrefix table prefix
   */
  public void writeOutput(GraphStore graphStore, String outputDir, String
    tablePrefix) throws Exception {
    Neo4jLineWriter writer = new Neo4jLineWriter(outputDir);
    GraphDatabaseService db = writer.getGraphDbService();
    String verticesTable = tablePrefix + GConstants.DEFAULT_TABLE_VERTICES;
    String graphsTable = tablePrefix + GConstants.DEFAULT_TABLE_GRAPHS;

    ArrayList<Long> allComponentsList = getAllComponents(graphStore,
      graphsTable);

    LOG.info("Creating Neo4j nodes...");
    Iterator<Vertex> vertices = graphStore.getVertices(verticesTable);
    writeVerticesToDb(writer, db, allComponentsList, vertices);

    LOG.info("Creating Neo4j relationships...");
    Iterator<Vertex> relVertices = graphStore.getVertices(verticesTable);
    writeRelationships(writer, db, allComponentsList, relVertices);

    writer.shutdown();
  }

  /**
   * Get all graph ids in a graphs table.
   * @param graphStore HBase graph store
   * @param graphsTable graphs table name
   * @return list with all graph ids
   * @throws Exception
   */
  private ArrayList<Long> getAllComponents(GraphStore graphStore,
    String graphsTable) throws Exception {
    Iterator<Graph> itGraphsTableElements = graphStore.getGraphs(graphsTable);
    HashMap<Long, Integer> graphs = restrictGraphs(itGraphsTableElements);

    return Lists.newArrayList(graphs.keySet());
  }

  /**
   * Write all relationships to the Neo4j embedded database instance.
   * @param writer Neo4j line writer to use
   * @param db graph database service
   * @param checkList list containing all components to be handled
   * @param vertices iterator vertices (containing relationships)
   */
  private void writeRelationships(Neo4jLineWriter writer,
    GraphDatabaseService db, ArrayList<Long> checkList,
    Iterator<Vertex> vertices) {
    int arrayCount = 0;
    int count = 0;
    int delay = 0;
    int windowSize = 500;
    ArrayList<Vertex> vertexList = new ArrayList<>(windowSize);
    while (vertices.hasNext()) {
      Vertex vertex = vertices.next();
      long vertexGraphComponent = vertex.getGraphs().iterator().next();

      if (checkList.contains(vertexGraphComponent)) {
        vertexList.add(vertex);
        ++arrayCount;
        if (arrayCount == windowSize) {
          try (Transaction tx = db.beginTx()) {
            for (Vertex v : vertexList) {
              writer.writeEdges(v);
            }
            tx.success();
          }
          arrayCount = 0;
          vertexList = new ArrayList<>(windowSize);
        }

        ++count;
        if (delay * windowSize < count) {
          ++delay;
          LOG.info("Relationships added to List Neo4j: " + count);
        }
      }
    }
    try (Transaction tx = db.beginTx()) {
      for (Vertex vertex : vertexList) {
        writer.writeEdges(vertex);
      }
      tx.success();
    }
    LOG.info("Relationships created in Neo4j: " + count);
  }

  /**
   * Write all vertices to the Neo4j embedded database instance.
   * @param writer Neo4j line writer to use
   * @param db graph database service
   * @param checkList list containing all components to be handled
   * @param vertices iterator vertices
   */
  private void writeVerticesToDb(Neo4jLineWriter writer,
    GraphDatabaseService db, ArrayList checkList, Iterator<Vertex> vertices) {
    int count = 0;
    int delay = 0;
    int windowSize = 1000;
    int arrayCount = 0;
    ArrayList<Vertex> vl = new ArrayList<>(windowSize);

    while (vertices.hasNext()) {
      Vertex v = vertices.next();
      if (checkList.contains(v.getGraphs().iterator().next())) {
        vl.add(v);
        ++arrayCount;
        if (arrayCount == windowSize) {
          writeListToDb(writer, db, vl);
          arrayCount = 0;
          vl = new ArrayList<>(windowSize);
        }
        ++count;
        if (delay * windowSize < count) {
          ++delay;
          LOG.info("Nodes created in Neo4j: " + count);
        }
      }
    }
    writeListToDb(writer, db, vl);
    LOG.info("Nodes created in Neo4j: " + count);
  }

  /**
   * Transaction of an array list containing vertices.
   * @param writer Neo4j line writer
   * @param db graph database service
   * @param vl vertex list
   */
  private void writeListToDb(Neo4jLineWriter writer, GraphDatabaseService db,
    ArrayList<Vertex> vl) {
    try (Transaction tx = db.beginTx()) {
      for (Vertex vertex : vl) {
        writer.writeVertex(vertex);
      }
      tx.success();
    }
  }

  /**
   * Restrict the set of graphs to graphs containing at least 5 elements.
   * @param graphs iterator element over all graphs
   * @throws Exception
   * @return hash map containing <componentId, size> entries
   */
  private HashMap<Long, Integer> restrictGraphs(Iterator<Graph> graphs)
      throws Exception {
    int restrictSize = 5;
    HashMap<Long, Integer> graphMap = new HashMap<>();
    while (graphs.hasNext()) {
      Graph graph = graphs.next();
      int count = (int) graph.getProperty(
        SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);
      if (count > restrictSize) {
        graphMap.put(graph.getID(), count);
      }
    }

    return graphMap;
  }

  /**
   * Runs the job from console.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Neo4jOutputDriver(), args));
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
    public static final String OPTION_NEO4J_OUTPUT_PATH = "o";
    /**
     * Create custom vertices table for different use cases.
     */
    public static final String OPTION_TABLE_PREFIX = "tp";
    /**
     * Holds options accepted by
     * {@link org.gradoop.rdf.examples.Neo4jOutputDriver}.
     */
    private static Options OPTIONS;

    static {
      OPTIONS = new Options();
      OPTIONS.addOption(OPTION_HELP, "help", false, "Display help.");
      OPTIONS.addOption(OPTION_VERBOSE, "verbose", false,
        "Print console output during job execution.");
      OPTIONS.addOption(OPTION_NEO4J_OUTPUT_PATH, "neo4j-output-path", true,
        "Path where the output will be stored.");
      OPTIONS.addOption(OPTION_TABLE_PREFIX, "table-prefix",
        true, "Custom prefix for HBase table to distinguish different use " +
          "cases.");
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
      formatter.printHelp(ConfUtils.class.getSuperclass().getName(), OPTIONS,
        true);
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
