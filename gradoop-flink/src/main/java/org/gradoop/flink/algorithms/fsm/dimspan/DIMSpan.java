/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.dimspan;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.AlphabeticalLabelComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.InverseProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.LabelComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.ProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DictionaryType;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.DFSCodeToEPGMGraphTransaction;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.CreateCollector;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.ExpandFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.GrowFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.InitSingleEdgePatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.IsFrequentPatternCollector;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.NotObsolete;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.ReportSupportedPatterns;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.VerifyPattern;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.CompressPattern;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.AggregateMultipleFunctions;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.CreateDictionary;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.EncodeAndPruneEdges;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.EncodeAndPruneVertices;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.MinFrequency;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.NotEmpty;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.ReportEdgeLabels;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing.ReportVertexLabels;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.DirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.UndirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphIntString;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class DIMSpan {

  /**
   * Maximum number of iterations if set of k-edge frequent patterns is not running empty before.
   */
  private static final int MAX_ITERATIONS = 100;

  /**
   * FSM configuration
   */
  protected final DIMSpanConfig fsmConfig;

  /**
   * input graph collection cardinality
   */
  protected DataSet<Long> graphCount;

  /**
   * minimum frequency for patterns to be considered to be frequent
   */
  protected DataSet<Long> minFrequency;

  /**
   * Pattern growth and verification logic derived from gSpan.
   * See <a href="https://www.cs.ucsb.edu/~xyan/software/gSpan.htm">gSpan</a>
   */
  protected final GSpanLogic gSpan;

  /**
   * Vertex label dictionary for dictionary coding.
   */
  private DataSet<String[]> vertexDictionary;

  /**
   * Edge label dictionary for dictionary coding.
   */
  private DataSet<String[]> edgeDictionary;

  /**
   * Label comparator used for dictionary coding.
   */
  private final LabelComparator comparator;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public DIMSpan(DIMSpanConfig fsmConfig) {
    this.fsmConfig = fsmConfig;

    // set gSpan implementation depending on direction mode
    gSpan = fsmConfig.isDirected() ?
      new DirectedGSpanLogic(fsmConfig) :
      new UndirectedGSpanLogic(fsmConfig);

    // set comparator based on dictionary type
    if (fsmConfig.getDictionaryType() == DictionaryType.PROPORTIONAL) {
      comparator =  new ProportionalLabelComparator();
    } else if (fsmConfig.getDictionaryType() == DictionaryType.INVERSE_PROPORTIONAL) {
      comparator =  new InverseProportionalLabelComparator();
    } else {
      comparator = new AlphabeticalLabelComparator();
    }
  }

  /**
   * Executes the DIMSpan algorithm.
   * Orchestration of preprocessing, mining and postprocessing.
   *
   * @param input input graph collection
   * @return frequent patterns
   */
  public DataSet<GraphTransaction> execute(DataSet<LabeledGraphStringString> input) {

    DataSet<int[]> encodedInput = preProcess(input);
    DataSet<WithCount<int[]>> encodedOutput = mine(encodedInput);

    return postProcess(encodedOutput);
  }

  /**
   * Triggers the label-frequency base preprocessing
   *
   * @param graphs input
   * @return preprocessed input
   */
  private DataSet<int[]> preProcess(DataSet<LabeledGraphStringString> graphs) {

    // Determine cardinality of input graph collection
    this.graphCount = Count
      .count(graphs);

    // Calculate minimum frequency
    this.minFrequency = graphCount
      .map(new MinFrequency(fsmConfig));

    // Execute vertex label pruning and dictionary coding
    DataSet<LabeledGraphIntString> graphsWithEncodedVertices = encodeVertices(graphs);

    // Execute edge label pruning and dictionary coding
    DataSet<int[]> encodedGraphs = encodeEdges(graphsWithEncodedVertices);

    // return all non-obsolete encoded graphs
    return encodedGraphs
      .filter(new NotEmpty());
  }

  /**
   * Triggers the iterative mining process.
   *
   * @param graphs preprocessed input graph collection
   * @return frequent patterns
   */
  protected DataSet<WithCount<int[]>> mine(DataSet<int[]> graphs) {

    DataSet<GraphWithPatternEmbeddingsMap> searchSpace = graphs
      .map(new InitSingleEdgePatternEmbeddingsMap(gSpan, fsmConfig));

    // Workaround to support multiple data sinks: create pseudo-graph (collector),
    // which embedding map will be used to union all k-edge frequent patterns
    DataSet<GraphWithPatternEmbeddingsMap> collector = graphs
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new CreateCollector());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<GraphWithPatternEmbeddingsMap> iterative = searchSpace
      .iterate(MAX_ITERATIONS);

    // ITERATION BODY

    DataSet<WithCount<int[]>> reports = iterative
      .flatMap(new ReportSupportedPatterns());

    DataSet<WithCount<int[]>> frequentPatterns = getFrequentPatterns(reports);

    DataSet<GraphWithPatternEmbeddingsMap> grownEmbeddings = iterative
      .map(new GrowFrequentPatterns(gSpan, fsmConfig))
      .withBroadcastSet(frequentPatterns, DIMSpanConstants.FREQUENT_PATTERNS)
      .filter(new NotObsolete());

    // ITERATION FOOTER

    return iterative
      .closeWith(grownEmbeddings, frequentPatterns)
      // keep only collector and expand embedding map keys
      .filter(new IsFrequentPatternCollector())
      .flatMap(new ExpandFrequentPatterns());
  }

  /**
   * Triggers the postprocessing.
   *
   * @param encodedOutput frequent patterns represented by multiplexed int-arrays
   * @return Gradoop graph transactions
   */
  private DataSet<GraphTransaction> postProcess(DataSet<WithCount<int[]>> encodedOutput) {
    return encodedOutput
      .map(new DFSCodeToEPGMGraphTransaction(fsmConfig))
      .withBroadcastSet(vertexDictionary, DIMSpanConstants.VERTEX_DICTIONARY)
      .withBroadcastSet(edgeDictionary, DIMSpanConstants.EDGE_DICTIONARY)
      .withBroadcastSet(graphCount, DIMSpanConstants.GRAPH_COUNT);
  }

  /**
   * Executes pruning and dictionary coding of vertex labels.
   *
   * @param graphs graphs with string-labels
   * @return graphs with dictionary-encoded vertex labels
   */
  private DataSet<LabeledGraphIntString> encodeVertices(DataSet<LabeledGraphStringString> graphs) {

    // LABEL PRUNING

    DataSet<WithCount<String>> vertexLabels = graphs
      .flatMap(new ReportVertexLabels());

    vertexLabels = getFrequentLabels(vertexLabels);

    // DICTIONARY ENCODING

    vertexDictionary = vertexLabels
      .reduceGroup(new CreateDictionary(comparator));

    return graphs
      .map(new EncodeAndPruneVertices())
      .withBroadcastSet(vertexDictionary, DIMSpanConstants.VERTEX_DICTIONARY);
  }

  /**
   * Executes pruning and dictionary coding of edge labels.
   *
   * @param graphs graphs with dictionary-encoded vertex labels
   * @return graphs with dictionary-encoded vertex and edge labels
   */
  private DataSet<int[]> encodeEdges(DataSet<LabeledGraphIntString> graphs) {

    DataSet<WithCount<String>> edgeLabels = graphs
      .flatMap(new ReportEdgeLabels());

    edgeLabels = getFrequentLabels(edgeLabels);

    edgeDictionary = edgeLabels
      .reduceGroup(new CreateDictionary(comparator));

    return graphs
      .map(new EncodeAndPruneEdges(fsmConfig))
      .withBroadcastSet(edgeDictionary, DIMSpanConstants.EDGE_DICTIONARY);
  }

  /**
   * Determines frequent labels.
   *
   * @param labels dataset of labels
   *
   * @return dataset of frequent labels
   */
  private DataSet<WithCount<String>> getFrequentLabels(DataSet<WithCount<String>> labels) {
    // enabled
    if (fsmConfig.getDictionaryType() != DictionaryType.RANDOM) {

      labels = labels
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<>())
        .withBroadcastSet(minFrequency, DIMSpanConstants.MIN_FREQUENCY);

      // disabled
    } else {
      labels = labels
        .distinct();
    }

    return labels;
  }

  /**
   * Identifies valid frequent patterns from a dataset of reported patterns.
   *
   * @param patterns reported patterns
   * @return valid frequent patterns
   */
  private DataSet<WithCount<int[]>> getFrequentPatterns(DataSet<WithCount<int[]>> patterns) {
    // COMBINE

    patterns = patterns
      .groupBy(0)
      .combineGroup(sumPartition());

    if (fsmConfig.getPatternVerificationInStep() == DataflowStep.COMBINE) {
      patterns = patterns
        .filter(new VerifyPattern(gSpan, fsmConfig));
    }

    if (fsmConfig.getPatternCompressionInStep() == DataflowStep.COMBINE) {
      patterns = patterns
        .map(new CompressPattern());
    }

    // REDUCE

    patterns = patterns
      .groupBy(0)
      .sum(1);

    // FILTER

    patterns = patterns
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, DIMSpanConstants.MIN_FREQUENCY);

    if (fsmConfig.getPatternVerificationInStep() == DataflowStep.FILTER) {
      patterns = patterns
        .filter(new VerifyPattern(gSpan, fsmConfig));
    }

    if (fsmConfig.getPatternCompressionInStep() == DataflowStep.FILTER) {
      patterns = patterns
        .map(new CompressPattern());
    }

    return patterns;
  }

  /**
   * Creates a Flink sum aggregate function that can be applied in group combine operations.
   *
   * @return sum group combine function
   */
  private GroupCombineFunction<WithCount<int[]>, WithCount<int[]>> sumPartition() {

    @SuppressWarnings("unchecked")
    AggregationFunction<Long>[] sum = new AggregationFunction[] {
      new SumAggregationFunction.SumAggregationFunctionFactory()
        .createAggregationFunction(Long.class)
    };

    int[] fields = { 1 };

    return new AggregateMultipleFunctions(sum, fields);
  }

  public String getName() {
    return this.getClass().getSimpleName();
  }

}
