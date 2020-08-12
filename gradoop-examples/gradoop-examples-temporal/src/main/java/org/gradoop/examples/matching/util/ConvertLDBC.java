package org.gradoop.examples.matching.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;

public class ConvertLDBC {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Option to declare path to temporal input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option for output path (results are written in a TemporalCSV sink)
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Output path
   */
  private static String OUTPUT_PATH;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Input path to indexed source files.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "outputPath", true,
      "output path for sink");
  }



  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args);

    if (cmd == null) {
      return;
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = getExecutionEnvironment();
    TemporalGradoopConfig config = TemporalGradoopConfig.fromGradoopFlinkConfig(
      GradoopFlinkConfig.createConfig(env));

    // read graph
    CSVDataSource source = new CSVDataSource(INPUT_PATH, config);

    TemporalGraph graph = TemporalGraph.fromGraph(source.getLogicalGraph());

    LocalDateTime now = LocalDateTime.now();
    ZoneId zone = ZoneId.of("Z");
    ZoneOffset offset = zone.getRules().getOffset(now);

    DataSet<TemporalEdge> edges = graph.getEdges().map(new MapFunction<TemporalEdge, TemporalEdge>() {
      @Override
      public TemporalEdge map(TemporalEdge value) throws Exception {
        if(value.hasProperty("creationDate")){
          value.setValidFrom(value.getPropertyValue("creationDate").getDateTime().toEpochSecond(offset)*1000L);
        } else if(value.hasProperty("joinDate")){
          value.setValidFrom(value.getPropertyValue("joinDate").getDateTime().toEpochSecond(offset)*1000L);
        }
        return value;
      }
    });

    DataSet<TemporalVertex> vertices = graph.getVertices().map(new MapFunction<TemporalVertex, TemporalVertex>() {
      @Override
      public TemporalVertex map(TemporalVertex value) throws Exception {
        if(value.hasProperty("creationDate")){
          value.setValidFrom(value.getPropertyValue("creationDate").getDateTime().toEpochSecond(offset)*1000L);
        }
        return value;
      }
    });

    graph = graph.getFactory().fromDataSets(vertices, edges);

    /*List<TemporalVertex> newVertices = graph.getVertices()
      .map(value -> {
        if(value.hasProperty("creationDate")){
          value.setValidFrom(value.getPropertyValue("creationDate").getDateTime().toEpochSecond(offset)*1000L);
        }
        return value;
      }).collect();



    List<TemporalEdge> newEdges = graph.getEdges()
      .map(value -> {
        if(value.hasProperty("creationDate")){
          try {
            value.setValidFrom(value.getPropertyValue("creationDate").getDateTime().toEpochSecond(offset)*1000L);
          } catch(Exception e){
            System.out.println("Error converting edge "+value);
            throw e;
          }
        }
        return value;
      }).collect();
*/
    //graph = graph.getFactory().fromCollections(newVertices, newEdges);

    TemporalCSVDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, config);
    sink.write(graph, true);
    env.execute();

  }

  protected static CommandLine parseArguments(String[] args)
    throws ParseException {
    return new DefaultParser().parse(OPTIONS, args);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a output graph directory");
    }
  }


  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH, null);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, null);
    if(INPUT_PATH==null || OUTPUT_PATH==null){
      throw new IllegalArgumentException("Please specify input and output path");
    }
  }
}
