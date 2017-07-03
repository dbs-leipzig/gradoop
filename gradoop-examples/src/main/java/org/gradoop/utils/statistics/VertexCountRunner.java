
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;

/**
 * Computes {@link VertexCount} for a given logical graph.
 */
public class VertexCountRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new VertexCount()
      .execute(readLogicalGraph(args[0], args[1]))
      .map(new ObjectTo1<>())
      .writeAsCsv(appendSeparator(args[2]) + GraphStatisticsReader.FILE_VERTEX_COUNT,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Vertex count");
  }

  @Override
  public String getDescription() {
    return VertexCountRunner.class.getName();
  }
}
