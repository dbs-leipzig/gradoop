package org.gradoop.examples.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.statistics.GraphStatisticsDataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class StatisticsExample extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    DataSource source = new CSVDataSource(args[0], config);
    DataSink sink = new GraphStatisticsDataSink(args[1]);

    sink.write(source.getLogicalGraph());
    env.execute();
  }

  @Override
  public String getDescription() {
    return StatisticsExample.class.getName();
  }
}
