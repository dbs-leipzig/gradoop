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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.nesting;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.gradoop.benchmark.nesting.functions.ExtractIdFromLest;
import org.gradoop.benchmark.nesting.functions.ImportVertexId;
import org.gradoop.benchmark.nesting.functions.InitEdgeForCollection;
import org.gradoop.benchmark.nesting.functions.InitVertexForCollection;
import org.gradoop.benchmark.nesting.functions.IsLeftOperand;
import org.gradoop.benchmark.nesting.functions.UpdateEdgeEdgeIdPreserving;
import org.gradoop.benchmark.nesting.serializers.DataSinkGradoopId;
import org.gradoop.benchmark.nesting.serializers.DataSinkTupleOfGradoopId;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.AddElementToGraph2;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for traverser benchmarks
 */
public class LogicalGraphToNestedModel extends AbstractRunner {
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_SUBGRAPHS_FOLDER = "s";
  /**
   * Path to CSV log file
   */
  private static final String OUTPUT_MODEL = "o";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph File in gMark format");
    OPTIONS.addOption(OPTION_SUBGRAPHS_FOLDER, "sub", true, "Graph Folder containing subgraphs" +
      " of the main input in gMark format. Each file could be either a vertex file (*-edges.txt) " +
      "or an edge one (*-vertices.txt)");
    OPTIONS.addOption(OUTPUT_MODEL, "out", true, "Path to output in nested format");
  }

  /**
   * Path to input graph data
   */
  private static String inputPath;
  /**
   * Query to execute.
   */
  private static String subgraphs;
  /**
   * Path to output
   */
  private static String outPath;

  /**
   *
   */
  private GraphModelConstructor constructor;

  /**
   * Default vertex factory
   */
  private final VertexFactory vFac;

  /**
   * Default edge factory
   */
  private final EdgeFactory eFac;

  /**
   * Checks if a vertex appears in the graph head
   */
  private final InGraph<Vertex> vertexInGraphNULL;

  /**
   * Checks if an edge appears in a graph head
   */
  private final InGraph<Edge> edgeInGraphNULL;


  private final GradoopFlinkConfig conf;

  /**
   * Initializes the main class using the main graph implementing the flattened graph
   * @param conf  Path to the graph containing all the possible information
   */
  public LogicalGraphToNestedModel(GradoopFlinkConfig conf, GraphModelConstructor constructor) {
    // Defining the basic configurations for the elements' generators
    this.conf = conf;
    vFac = conf.getVertexFactory();
    eFac = conf.getEdgeFactory();
    // Defining the standard way to write an element to a file
    this.constructor = constructor;

    vertexInGraphNULL = new InGraph<>(GradoopId.NULL_VALUE);
    edgeInGraphNULL = new InGraph<>(GradoopId.NULL_VALUE);
  }

  private LogicalGraph extractLeftOperand(GraphCollection selfie) {
    DataSet<GraphHead> head = constructor.getAllHeadInfos()
      .filter(new IsLeftOperand(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> vertices = selfie.getVertices()
      .joinWithTiny(head)
      .where((Vertex v) -> 0).equalTo((GraphHead x)-> 0)
      .with(vertexInGraphNULL);

    DataSet<Edge> edges = selfie.getEdges()
      .joinWithTiny(head)
      .where((Edge v) -> 0).equalTo((GraphHead x)-> 0)
      .with(edgeInGraphNULL);

    return LogicalGraph.fromDataSets(head,vertices,edges,conf);
  }

  private GraphCollection extractRightOperand(GraphCollection selfie) {
    DataSet<GraphHead> head = constructor.getAllHeadInfos()
      .filter(new IsLeftOperand(false))
      .map(new Value2Of3<>());

    DataSet<Vertex> vertices = selfie.getVertices()
      .joinWithTiny(head)
      .where((Vertex v) -> 0).equalTo((GraphHead x)-> 0)
      .with(vertexInGraphNULL);

    DataSet<Edge> edges = selfie.getEdges()
      .joinWithTiny(head)
      .where((Edge v) -> 0).equalTo((GraphHead x)-> 0)
      .with(edgeInGraphNULL);

    return GraphCollection.fromDataSets(head,vertices,edges,conf);
  }

  public void serializeOperandsWithData(String toPath) throws Exception {
    GraphCollection coll = collectSourcesAsCollection();

    // Writing the ground truth over which extract the informations for the edge semantics
    writeGraphCollection(coll,toPath);

    // Operand representing the graph for the search graph
    NestingIndex left =
      NestingBase.createIndex(extractLeftOperand(coll));
    writeIndex(left,toPath+(toPath.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR)
      +"left");

    // Operand representing the collection of the elements that we want to use for the nesting
    NestingIndex right =
      NestingBase.createIndex(extractRightOperand(coll));
    writeIndex(right,toPath+(toPath.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR)
      +"left");
  }

  private void writeIndex(NestingIndex left, String s) {
    left.getGraphHeads().output(new DataSinkGradoopId(new Path(s+"-heads.bin")));
    left.getGraphHeadToVertex().output(new DataSinkTupleOfGradoopId(new Path(s+"-vertex.bin")));
    left.getGraphHeadToEdge().output(new DataSinkTupleOfGradoopId(new Path(s+"-edges.bin")));
  }

  /**
   * Returns a collection that is going to be used to read it as a GraphCollection
   * @return
   */
  public GraphCollection collectSourcesAsCollection() {
    InitVertexForCollection<String> vertexMapper =
      new InitVertexForCollection<>(vFac, null, TypeInformation.of(String.class));

    DataSet<Tuple3<String, GradoopId, Vertex>> tripleVertices = constructor.getAllVertices()
      .groupBy(new ImportVertexId<>())
      .combineGroup(vertexMapper);

    DataSet<Tuple2<String, GradoopId>> vertexIdPair = tripleVertices
      .map(new Project3To0And1<>());

    DataSet<Tuple2<String,GradoopId>> edgeKeyToGraphId = constructor.getAllEdges()
      .map(new ExtractIdFromLest());

    DataSet<Edge> epgmEdges = constructor.getAllEdges()
      .map(new Value0Of2<>())
      .distinct(0)
      .join(vertexIdPair)
      .where(1).equalTo(0)
      .with(new InitEdgeForCollection<>(eFac, null, TypeInformation.of(String.class)))
      .join(vertexIdPair)
      .where(0).equalTo(0)
      .with(new UpdateEdgeEdgeIdPreserving<>())
      .coGroup(edgeKeyToGraphId)
      .where(0).equalTo(0)
      .with(new AddElementToGraph2());

    DataSet<Vertex> epgmVertices = tripleVertices
      .map(new Value2Of3<>());

    DataSet<GraphHead> heads = constructor.getAllHeadInfos()
      .map(new Value2Of3<>());

    return GraphCollection.fromDataSets(heads,epgmVertices,epgmEdges,conf);
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, LogicalGraphToNestedModel.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    subgraphs = cmd.getOptionValue(OPTION_SUBGRAPHS_FOLDER);
    outPath = cmd.getOptionValue(OUTPUT_MODEL);

    // Reading the informations from the files
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    GraphHeadFactory headFactory = conf.getGraphHeadFactory();

    // Initializes the model with the left graph
    GraphModelConstructor modelData =
      GraphModelConstructor.createGraphInformation(inputPath,headFactory);

    // Loading the graphs appearing in the graph collection
    // 1. Returning each vertex and edge files association
    File[] filesInDirectory = new File(subgraphs).listFiles();
    Map<String, List<String>> l = filesInDirectory != null ?
      Arrays.stream(filesInDirectory)
        .map(File::getAbsolutePath)
        .collect(Collectors.groupingBy(x -> x
          .replaceAll("-edges\\.txt$", "")
          .replaceAll("-vertices\\.txt$", "")))
      :
      new HashMap<>();

    // 2. Scanning all the files pertaining to the same graph, throught the previous aggregation
    for (List<String> files : l.values()) {
      if (files.size()==2) {
        String edgeFile = null;
        String vertexFile = null;
        for (String s : files) {
          if (s.endsWith("-edges.txt")) {
            edgeFile = s;
          } else if (s.endsWith("-vertices.txt")) {
            vertexFile = s;
          }
        }
        if (edgeFile != null && vertexFile != null) {
          GraphModelConstructor leftOperandDelta = GraphModelConstructor
            .createGraphInformationWithVertices(edgeFile, vertexFile, headFactory);
          modelData.incrementalUpdateWith(leftOperandDelta);
        }
      }
    }

    // Initializing the model with the data
    LogicalGraphToNestedModel model = new LogicalGraphToNestedModel(conf, modelData);

    // Writing the data information with the id information to be fast loaded in memory
    model.serializeOperandsWithData(outPath);
    getExecutionEnvironment().execute("Serializing job");
  }

}
