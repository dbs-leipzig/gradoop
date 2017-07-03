
package org.gradoop.examples.sna;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;

import java.util.Arrays;

/**
 * The benchmark program executes the following workflow:
 *
 * 1) Extract subgraph with:
 *    - vertex predicate: must be of type 'Person'
 *    - edge predicate: must be of type 'knows'
 * 2) Group the subgraph using the vertex attributes 'city' and 'gender' and
 *    - count the number of vertices represented by each super vertex
 *    - count the number of edges represented by each super edge
 * 3) Aggregate the grouped graph:
 *    - add the total vertex count as new graph property
 *    - add the total edge count as new graph property
 */
public class SNABenchmark1 extends AbstractRunner implements
  ProgramDescription {

  /**
   * Runs the example program.
   *
   * Need a (possibly HDFS) input directory that contains
   *  - nodes.json
   *  - edges.json
   *  - graphs.json
   *
   * Needs a (possibly HDFS) output directory to write the resulting graph to.
   *
   * @param args args[0] = input dir, args[1] output dir
   * @throws Exception
   */
  @SuppressWarnings({
    "unchecked",
    "Duplicates"
  })
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
      args.length == 2, "input dir and output dir required");
    String inputDir  = args[0];
    String outputDir = args[1];

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);

    LogicalGraph result = execute(epgmDatabase);

    writeLogicalGraph(result, outputDir);
  }

  /**
   * The actual computation.
   *
   * @param socialNetwork social network graph
   * @return summarized, aggregated graph
   */
  private static LogicalGraph execute(LogicalGraph socialNetwork) {
    return socialNetwork
      .subgraph(
        vertex -> vertex.getLabel().equals("person"),
        edge -> edge.getLabel().equals("knows"))
      .groupBy(Arrays.asList("gender", "city"))
      .aggregate(new VertexCount())
      .aggregate(new EdgeCount());
  }

  @Override
  public String getDescription() {
    return SNABenchmark1.class.getName();
  }
}
