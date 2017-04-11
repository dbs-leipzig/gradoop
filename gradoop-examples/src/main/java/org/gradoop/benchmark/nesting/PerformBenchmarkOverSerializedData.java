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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.benchmark.nesting.serializers.DeserializeGradoopidFromFile;
import org.gradoop.benchmark.nesting.serializers.DeserializePairOfIdsFromFile;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.model.NestedModel;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Class implementing the serialization methods
 */
public class PerformBenchmarkOverSerializedData extends NestingFilenameConvention {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  private static final String PHASE_TO_BENCHMARK = "p";
  /**
   * Path to CSV log file
   */
  private static final String OUTPUT_EXPERIMENT = "o";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph File in the serialized format");
    OPTIONS.addOption(PHASE_TO_BENCHMARK, "phase", true, "Phase to benchmark.\n\t 1: only " +
      "loads the data.\n\t 2: loads the data and perform the computation");
    OPTIONS.addOption(OUTPUT_EXPERIMENT, "csv", true, "Where to append the experiment for the " +
      "benchmark");
  }

  /**
   * Path to input graph data
   */
  private static String INPUT_PATH;
  /**
   * Query to execute.
   */
  private static String SUBGRAPHS;
  /**
   * Path to output
   */
  private static String OUTPATH;

  /**
   * Global environment
   */
  private final ExecutionEnvironment environment;

  /**
   * Defines the base path where the informations are stored (operands + flattened graph)
   */
  private final String basePath;

  /**
   * Indices for the left operand
   */
  private NestingIndex leftOperand;

  /**
   * Indices for the right operand
   */
  private NestingIndex rightOperand;

  /**
   * Defines the data model where the operations are performed
   */
  private NestedModel model;

  public PerformBenchmarkOverSerializedData(String basePath) {
    environment = getExecutionEnvironment();
    this.basePath = basePath;
  }

  /**
   * Phase 1: Loads the operands from secondary memory
   */
  private void loadIndicesWithFlattenedGraph() {
    leftOperand = loadNestingIndex(generateOperandBasePath(basePath,true));
    rightOperand = loadNestingIndex(generateOperandBasePath(basePath,false));
    NestingIndex nestedRepresentation = NestingBase.mergeIndices(leftOperand, rightOperand);
    LogicalGraph flat = readLogicalGraph(basePath);
    model = new NestedModel(flat, nestedRepresentation);
  }

  /**
   * Phase 2: evaluating the operator
   */
  private void runOperator() {
    model.nesting(leftOperand, rightOperand, GradoopId.get());
    model.disjunctiveSemantics(model.getPreviousResult(), rightOperand);
  }

  public void run(int maxPhase) throws Exception {
    int countPhase = 0;
    // Phase 1: Loading the operands
    loadIndicesWithFlattenedGraph();
    countPhase++;
    checkPhaseAndEvantuallyExit(countPhase, maxPhase);

    // Phase 2: Executing the actual operator
    runOperator();
    countPhase++;
    checkPhaseAndEvantuallyExit(countPhase, countPhase);
  }

  private void indexCount(NestingIndex index) {
    index.getGraphHeads().output(new Bogus<>());
    index.getGraphHeadToEdge().output(new Bogus<>());
    index.getGraphHeadToVertex().output(new Bogus<>());
  }

  private void finalizePhase(int toFinalize) throws IOException {
    if (toFinalize == 1) {
      // Counting each element for the loaded index, alongside with the values of the flattened
      // graph
      indexCount(leftOperand);
      indexCount(rightOperand);
      model.getFlattenedGraph().getGraphHead().output(new Bogus<>());
      model.getFlattenedGraph().getVertices().output(new Bogus<>());
      model.getFlattenedGraph().getEdges().output(new Bogus<>());
    } else if (toFinalize == 2)  {
      // Counting the computation actually required to produce the result, that is the graph stack
      // Alongside with the resulting indices
      NestingResult result = model.getPreviousResult();
      result.getGraphStack().output(new Bogus<>());
      indexCount(result);
    }
  }

  /**
   * Checks if we have now to stop and, eventually, stops the computation
   * @param countPhase    current phase
   * @param maxPhase      Maximum to be reached.
   * @throws Exception
   */
  private void checkPhaseAndEvantuallyExit(int countPhase, int maxPhase) throws Exception {
    if (countPhase == maxPhase) {
      finalizePhase(countPhase);
      // Writing the result of the benchmark to the file
      Files.write(Paths.get(OUTPUT_EXPERIMENT), (countPhase+","+environment.execute()
        .getNetRuntime()).getBytes(), StandardOpenOption.APPEND);
      // Exit the whole program, providing the phase no as a return number
      System.exit(countPhase);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SerializeData.class.getName());
    int phase = Integer.valueOf(cmd.getOptionValue(PHASE_TO_BENCHMARK));
    if (cmd == null) {
      System.exit(1);
    }

    PerformBenchmarkOverSerializedData benchmark = new PerformBenchmarkOverSerializedData
      (cmd.getOptionValue(OPTION_INPUT_PATH));
    benchmark.run(phase);
  }

  /**
   * Loads an index located in a given specific folder + operand prefix
   * @param filename  Foder
   * @return
   */
  private NestingIndex loadNestingIndex(String filename) {
    DataSet<GradoopId> headers = environment
      .readFile(new DeserializeGradoopidFromFile(), filename + INDEX_HEADERS_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> vertexIndex = environment
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_VERTEX_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> edgeIndex = environment
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_EDGE_SUFFIX);

    return new NestingIndex(headers, vertexIndex, edgeIndex);
  }

}
