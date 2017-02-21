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

package org.gradoop.examples.dimspan.dimspan;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.gradoop.examples.dimspan.dimspan.comparison.AlphabeticalLabelComparator;
import org.gradoop.examples.dimspan.dimspan.comparison.InverseProportionalLabelComparator;
import org.gradoop.examples.dimspan.dimspan.comparison.LabelComparator;
import org.gradoop.examples.dimspan.dimspan.comparison.ProportionalLabelComparator;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConfig;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConstants;
import org.gradoop.examples.dimspan.dimspan.config.DictionaryType;
import org.gradoop.examples.dimspan.dimspan.functions.preprocessing.ReportEdgeLabels;
import org.gradoop.examples.dimspan.dimspan.functions.preprocessing.EncodeAndPruneEdges;
import org.gradoop.examples.dimspan.dimspan.functions.preprocessing.EncodeAndPruneVertices;
import org.gradoop.examples.dimspan.dimspan.functions.MinFrequency;
import org.gradoop.examples.dimspan.dimspan.functions.NotEmpty;
import org.gradoop.examples.dimspan.dimspan.functions.preprocessing.CreateDictionary;
import org.gradoop.examples.dimspan.dimspan.functions.preprocessing.ReportVertexLabels;
import org.gradoop.examples.dimspan.dimspan.gspan.DirectedGSpanAlgorithm;
import org.gradoop.examples.dimspan.dimspan.gspan.GSpanAlgorithm;
import org.gradoop.examples.dimspan.dimspan.gspan.UndirectedGSpanAlgorithm;
import org.gradoop.examples.dimspan.dimspan.tuples.LabeledGraphIntString;
import org.gradoop.examples.dimspan.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.Frequent;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class DIMSpan {

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
   * Gradoop configuration
   */
  protected GradoopFlinkConfig gradoopFlinkConfig;

  /**
   * Pattern growth and verification logic derived from gSpan.
   * See <a href="https://www.cs.ucsb.edu/~xyan/software/gSpan.htm">gSpan</a>
   */
  protected final GSpanAlgorithm gSpan;

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
      new DirectedGSpanAlgorithm(fsmConfig) :
      new UndirectedGSpanAlgorithm(fsmConfig);

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
    DataSet<int[]> encodedOutput = mine(encodedInput);

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

  protected DataSet<int[]> mine(DataSet<int[]> graphs) {

    DataSet<GraphEmbeddingsPair> searchSpace = graphs
      .map(new InitSingleEdgeEmbeddings(gSpan, fsmConfig));

    DataSet<GraphEmbeddingsPair> collector = graphs
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new CreateCollector());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<GraphEmbeddingsPair> iterative = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    DataSet<WithCount<int[]>> reports = iterative
      .flatMap(new Report(fsmConfig));

    DataSet<int[]> frequentPatterns = getFrequentPatterns(reports);

    DataSet<GraphEmbeddingsPair> grownEmbeddings = iterative
      .map(new PatternGrowth(gSpan, fsmConfig))
      .withBroadcastSet(frequentPatterns, DIMSpanConstants.FREQUENT_PATTERNS)
      .filter(new HasEmbeddings());

    // ITERATION FOOTER

    return iterative
      .closeWith(grownEmbeddings, frequentPatterns)
      .filter(new IsCollector())
      .flatMap(new ExpandResult());
  }

  private DataSet<GraphTransaction> postProcess(DataSet<int[]> encodedOutput) {
    return encodedOutput
      .map(new DfsCodeToSetPair(gSpan))
      .map(new SetPairToGraphTransaction())
      .withBroadcastSet(vertexDictionary, DIMSpanConstants.FREQUENT_VERTEX_LABELS)
      .withBroadcastSet(edgeDictionary, DIMSpanConstants.FREQUENT_EDGE_LABELS);
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

    vertexLabels = pruneLabels(vertexLabels);

    // DICTIONARY ENCODING

    vertexDictionary = vertexLabels
      .reduceGroup(new CreateDictionary(comparator));

    return graphs
      .map(new EncodeAndPruneVertices())
      .withBroadcastSet(vertexDictionary, DIMSpanConstants.FREQUENT_VERTEX_LABELS);
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

    edgeLabels = pruneLabels(edgeLabels);

    edgeDictionary = edgeLabels
      .reduceGroup(new CreateDictionary(comparator));

    return graphs
      .map(new EncodeAndPruneEdges(fsmConfig))
      .withBroadcastSet(edgeDictionary, DIMSpanConstants.FREQUENT_EDGE_LABELS);
  }

  private DataSet<WithCount<String>> pruneLabels(DataSet<WithCount<String>> labels) {
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
  protected DataSet<int[]> getFrequentPatterns(DataSet<WithCount<int[]>> patterns) {
    // COMBINE

    patterns = patterns
      .groupBy(0)
      .combineGroup(sumPartition());

    if (fsmConfig.getPatternValidationInStep() == DataflowStep.COMBINE) {
      patterns = patterns
        .filter(new Validate(gSpan, fsmConfig));
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

    if (fsmConfig.getPatternValidationInStep() == DataflowStep.FILTER) {
      patterns = patterns
        .filter(new Validate(gSpan, fsmConfig));
    }

    if (fsmConfig.getPatternCompressionInStep() == DataflowStep.FILTER) {
      patterns = patterns
        .map(new CompressPattern());
    }

    return patterns
      .map(new ValueOfWithCount<>());
  }

  /**
   * Creates a Flink sum aggregate function that can be applied in group combine operations.
   *
   * @return sum group combine function
   */
  protected GroupCombineFunction<WithCount<int[]>, WithCount<int[]>>
  sumPartition() {

    SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };

    int[] fields = { 1 };

    return new AggregateOperator.AggregatingUdf(sum, fields);
  }

  public String getName() {
    return this.getClass().getSimpleName();
  }

}
