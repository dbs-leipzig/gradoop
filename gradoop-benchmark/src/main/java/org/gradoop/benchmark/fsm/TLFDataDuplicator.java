package org.gradoop.benchmark.fsm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tlf.TLFDataSink;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.functions.utils.Duplicate;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

public class TLFDataDuplicator
  extends AbstractRunner implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_MULTIPLICAND = "m";

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TLFDataDuplicator.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> config =
      GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment());

    String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    int multiplicand = Integer.valueOf(cmd.getOptionValue(OPTION_MULTIPLICAND));

    String outputPath = inputPath.replace(".tlf", "_" + multiplicand + ".tlf");


    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(inputPath, config);

    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new TLFDataSink<>(outputPath, config);

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> input =
      dataSource.getGraphTransactions();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new GraphTransactions<>(
        input
          .getTransactions()
          .flatMap(new Duplicate
            <GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo>>
            (multiplicand)),
        config
      );

    dataSink.write(output);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Input file must be provided");
    }
    if (!cmd.hasOption(OPTION_MULTIPLICAND)) {
      throw new IllegalArgumentException("Multiplicand must be provided.");
    }
  }

  @Override
  public String getDescription() {
    return TLFDataDuplicator.class.getName();
  }
}
