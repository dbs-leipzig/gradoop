package org.gradoop.utils.converter;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Converts a given graph into another gradoop format.
 */
public class GradoopFormatConverter extends AbstractRunner implements ProgramDescription {

  /**
   * Converts a graph from one gradoop format to another.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, json)
   * args[2] - path to output graph
   * args[3] - format of output graph (csv, json)
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);
    writeLogicalGraph(graph, args[2], args[3]);
  }

  @Override
  public String getDescription() {
    return GradoopFormatConverter.class.getName();
  }
}
