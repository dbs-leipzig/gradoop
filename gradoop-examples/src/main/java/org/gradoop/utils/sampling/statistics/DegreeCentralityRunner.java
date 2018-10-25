package org.gradoop.utils.sampling.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.sampling.statistics.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.DegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.writer.DegreeCentralityPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;
import org.gradoop.flink.model.impl.tuples.WithCount;

public class DegreeCentralityRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {

    DataSet<Double> dataSet = new DegreeCentrality()
      .execute(readLogicalGraph(args[0], args[1]));
    dataSet.print();

    StatisticWriter.writeCSV(new DegreeCentralityPreparer()
        .execute(readLogicalGraph(args[0], args[1])),
      appendSeparator(args[2]) + SamplingEvaluationConstants.FILE_DEGREE_CENTRALITY);

    getExecutionEnvironment().execute("Statistics: Degree centrality");
  }

  @Override
  public String getDescription() {
    return DegreeCentralityRunner.class.getName();
  }
}
