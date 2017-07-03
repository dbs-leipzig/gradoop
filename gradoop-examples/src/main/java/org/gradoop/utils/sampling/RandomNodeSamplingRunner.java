
package org.gradoop.utils.sampling;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Runs {@link org.gradoop.flink.model.impl.operators.sampling.RandomNodeSampling} for a given
 * graph and writes the sampled output.
 */
public class RandomNodeSamplingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Runs the {@link org.gradoop.flink.model.impl.operators.sampling.RandomNodeSampling} operator
   * on the specified graph and writes the result to the specified output.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph
   * args[2] - vertex probability
   * args[3] - path to output
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    LogicalGraph sample = graph.sampleRandomNodes(Float.parseFloat(args[2]));
    new CSVDataSink(args[3], args[0] + "/metadata.csv", graph.getConfig()).write(sample);
    getExecutionEnvironment().execute("Random Node Sampling (" + args[2] + ")");
  }

  @Override
  public String getDescription() {
    return RandomNodeSamplingRunner.class.getName();
  }
}
