package org.gradoop.benchmark.nesting;

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

/**
 * Created by vasistas on 10/04/17.
 */
public class PerformBenchmarkOverSerializedData extends NestingFilenameConvention {

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
    leftOperand = loadNestingIndex(0, generateOperandBasePath(basePath,true));
    rightOperand = loadNestingIndex(1, generateOperandBasePath(basePath,false));
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
      indexCount(leftOperand);
      indexCount(rightOperand);
      model.getFlattenedGraph().getGraphHead().output(new Bogus<>());
      model.getFlattenedGraph().getVertices().output(new Bogus<>());
      model.getFlattenedGraph().getEdges().output(new Bogus<>());
    } else if (toFinalize == 2)  {
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
      System.out.println(countPhase+","+environment.execute().getNetRuntime());
      System.exit(countPhase);
    }
  }

  public static void main(String[] args) throws Exception {
    PerformBenchmarkOverSerializedData benchmark = new PerformBenchmarkOverSerializedData
      ("/Users/vasistas/test/");
    benchmark.run(2);
  }

  private NestingIndex loadNestingIndex(int idx, String filename) {

    DataSet<GradoopId> headers = environment
      .readFile(new DeserializeGradoopidFromFile(), filename + INDEX_HEADERS_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> vertexIndex = environment
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_VERTEX_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> edgeIndex = environment
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_EDGE_SUFFIX);

    return new NestingIndex(headers, vertexIndex, edgeIndex);
  }

}
