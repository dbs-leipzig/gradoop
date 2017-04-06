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
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.gradoop.benchmark.nesting.functions.AssociateNewGraphHead;
import org.gradoop.benchmark.nesting.functions.ExtendTupleWithGraphList;
import org.gradoop.benchmark.nesting.functions.ExtractVerticesFromEdges;
import org.gradoop.benchmark.nesting.functions.ImportEdgeTuplesId;
import org.gradoop.benchmark.nesting.functions.ImportVertexId;
import org.gradoop.benchmark.nesting.functions.InitEdgeForCollection;
import org.gradoop.benchmark.nesting.functions.InitVertexForCollection;
import org.gradoop.benchmark.nesting.functions.IsLeftOperand;
import org.gradoop.benchmark.nesting.functions.TripleSplit;
import org.gradoop.benchmark.nesting.parsers.ParametricInputFormat;
import org.gradoop.benchmark.nesting.serializers.DataSinkGradoopId;
import org.gradoop.benchmark.nesting.serializers.DataSinkTupleOfGradoopId;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.graph.functions.UpdateEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.transformations
  .EPGMToNestedIndexingTransformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Base class for traverser benchmarks
 */
public class LoadingNestedModel extends AbstractRunner {
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
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph directory");
    OPTIONS.addOption(OPTION_SUBGRAPHS_FOLDER, "sub", true, "Pattern or fixed query");
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
   * Default way to read strings from files
   */
  private final ParametricInputFormat pif;

  /**
   * Default header factory
   */
  private final GraphHeadFactory fac;

  /**
   * Default vertex factory
   */
  private final VertexFactory vFac;

  /**
   * Default edge factory
   */
  private final EdgeFactory eFac;

  /**
   * Information from all the headers from the graph
   */
  private DataSet<Tuple3<String, Boolean, GraphHead>> allHeadInfos;

  /**
   *
   */
  private DataSet<Tuple2<ImportEdge<String>,GradoopId>> allEdges;
  private DataSet<Tuple2<ImportVertex<String>,GradoopId>> allVertices;
  private final GradoopFlinkConfig conf;

  public LoadingNestedModel(String pathToDataLake) {
    conf = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    fac = conf.getGraphHeadFactory();
    vFac = conf.getVertexFactory();
    eFac = conf.getEdgeFactory();

    // Heads to be assoicated to the file
    allHeadInfos = getExecutionEnvironment().fromElements(pathToDataLake)
        .map(new AssociateNewGraphHead(fac,true));

    DataSet<GradoopId> head = allHeadInfos
      .map(new Value2Of3<>())
      .map(new Id<>());

    pif = new ParametricInputFormat();
    allEdges = getExecutionEnvironment().readFile(pif,pathToDataLake)
        .flatMap(new TripleSplit())
        .crossWithTiny(head);

    allVertices = allEdges
      .flatMap(new ExtractVerticesFromEdges<>())
      .distinct(new ImportVertexId<>());
  }

  public void addFile(String path) {
    // Heads to be assoicated to the file
    DataSet<Tuple3<String, Boolean, GraphHead>> headFullInfo =
      getExecutionEnvironment().fromElements(path)
        .map(new AssociateNewGraphHead(fac,false));
    allHeadInfos.union(headFullInfo);

    DataSet<GradoopId> head = headFullInfo
      .map(new Value2Of3<>())
      .map(new Id<>());

    DataSet<Tuple2<ImportEdge<String>,GradoopId>> rawEdges =
      getExecutionEnvironment().readFile(pif,path)
        .flatMap(new TripleSplit())
        .crossWithTiny(head);
    allEdges.union(rawEdges);

    DataSet<Tuple2<ImportVertex<String>,GradoopId>> rawVertices =
      rawEdges.flatMap(new ExtractVerticesFromEdges<>())
      .distinct(new ImportVertexId<>());

    allVertices.union(rawVertices);
  }

  private LogicalGraph extractLeftOperand(GraphCollection selfie) {
    DataSet<GraphHead> head = allHeadInfos
      .filter(new IsLeftOperand(true))
      .map(new Value2Of3<>());

    DataSet<Vertex> vertices = selfie.getVertices()
      .joinWithTiny(head)
      .where((Vertex v) -> 0).equalTo((GraphHead x)-> 0)
      .with(new InGraph<>(GradoopId.NULL_VALUE));

    DataSet<Edge> edges = selfie.getEdges()
      .joinWithTiny(head)
      .where((Edge v) -> 0).equalTo((GraphHead x)-> 0)
      .with(new InGraph<>(GradoopId.NULL_VALUE));

    return LogicalGraph.fromDataSets(head,vertices,edges,conf);
  }

  private GraphCollection extractRightOperand(GraphCollection selfie) {
    DataSet<GraphHead> head = allHeadInfos
      .filter(new IsLeftOperand(false))
      .map(new Value2Of3<>());

    DataSet<Vertex> vertices = selfie.getVertices()
      .joinWithTiny(head)
      .where((Vertex v) -> 0).equalTo((GraphHead x)-> 0)
      .with(new InGraph<>(GradoopId.NULL_VALUE));

    DataSet<Edge> edges = selfie.getEdges()
      .joinWithTiny(head)
      .where((Edge v) -> 0).equalTo((GraphHead x)-> 0)
      .with(new InGraph<>(GradoopId.NULL_VALUE));

    return GraphCollection.fromDataSets(head,vertices,edges,conf);
  }

  public void serializeOperandsWithData(String toPath) throws Exception {
    GraphCollection coll = collectSourcesAsCollection();

    // Writing the ground truth over which extract the informations for the edge semantics
    writeGraphCollection(coll,toPath);

    // Operand representing the graph
    NestingIndex left =
      EPGMToNestedIndexingTransformation.createIndex(extractLeftOperand(coll));

    // Operand representing the collection of the elements that we want to nest
    NestingIndex right =
      EPGMToNestedIndexingTransformation.createIndex(extractRightOperand(coll));

    writeIndex(left,toPath+(toPath.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR)
      +"left");
  }

  private void writeIndex(NestingIndex left, String s) {
    left.getGraphHeads().output(new DataSinkGradoopId(new Path(s+"-heads.bin")));
    left.getGraphHeadToVertex().output(new DataSinkTupleOfGradoopId(new Path(s+"-vertex.bin")));
    left.getGraphHeadToEdge().output(new DataSinkTupleOfGradoopId(new Path(s+"-vertex.bin")));
  }

  /**
   * Returns a collection that is going to be used to read it as a GraphCollection
   * @return
   */
  public GraphCollection collectSourcesAsCollection() {
    InitVertexForCollection<String> vertexMapper =
      new InitVertexForCollection<>(vFac, null, TypeInformation.of(String.class));

    DataSet<Tuple3<String, GradoopId, Vertex>> tripleVertices = allVertices
      .groupBy(new ImportVertexId<>())
      .combineGroup(vertexMapper);

    DataSet<Tuple2<String, GradoopId>> vertexIdPair = tripleVertices
      .map(new Project3To0And1<>());

    DataSet<Tuple2<ImportEdge<String>, GradoopIdList>> extendedEdges = allEdges
      .groupBy(new ImportEdgeTuplesId<>())
      .combineGroup(new ExtendTupleWithGraphList<>());

    DataSet<Edge> epgmEdges = extendedEdges
      .join(vertexIdPair)
      .where(1).equalTo(0)
      .with(new InitEdgeForCollection<>(eFac, null, TypeInformation.of(String.class)))
      .join(vertexIdPair)
      .where(0).equalTo(0)
      .with(new UpdateEdge<>());

    DataSet<Vertex> epgmVertices = tripleVertices
      .map(new Value2Of3<>());

    DataSet<GraphHead> heads = allHeadInfos.map(new Value2Of3<>());

    return GraphCollection.fromDataSets(heads,epgmVertices,epgmEdges,conf);
  }

  public void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, LoadingNestedModel.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    subgraphs = cmd.getOptionValue(OPTION_SUBGRAPHS_FOLDER);
    outPath = cmd.getOptionValue(OUTPUT_MODEL);

    // Reading the informations from the files
    LoadingNestedModel model = new LoadingNestedModel(inputPath);
    File folder = new File(subgraphs);
    File[] listOfFiles = folder.listFiles();
    for (File f : listOfFiles) {
      if (!f.isHidden() && f.isFile()) {
        model.addFile(f.getPath());
      }
    }

    // Writing the data information with the id information to be fast loaded in memory
    model.serializeOperandsWithData(outPath);
  }

  /**
   * Writes the results of the benchmark into the given csv file. If the file already exists,
   * the results are appended.
   *
   * @param csvFile path to csv file
   * @throws IOException
   */
  private void writeResults(String csvFile) throws IOException {
    String header = "Input|Parallelism|Strategy|Query|Embeddings|Runtime[ms]";
    String line = "";

    File f = new File(csvFile);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, String.format("%s%n", line), true);
    } else {
      PrintWriter writer = new PrintWriter(csvFile, "UTF-8");
      writer.println(header);
      writer.println(line);
      writer.close();
    }
  }

  /**
   * Writes the results to file or prints it.
   *
   * @throws IOException
   */
  void close() throws IOException {
    if (outPath != null) {
      writeResults(outPath);
    }
  }
}
