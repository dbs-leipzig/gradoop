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

package org.gradoop.benchmark.fsm;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.fsm.common.config.CanonicalLabel;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.GrowthStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;
import org.gradoop.flink.algorithms.fsm.tfsm.TransactionalFSM;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized transactional FSM benchmark.
 */
public class TFSMBenchmark
  extends AbstractRunner implements ProgramDescription {

  static {
    for (TFSMParam param : TFSMParam.values()) {
      OPTIONS.addOption(param.toString(), param.toString());
    }
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TFSMBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read from source
    GradoopFlinkConfig gradoopConfig = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    String inputPath = cmd.getOptionValue(TFSMParam.i.toString());

    System.out.println(inputPath);

    TLFDataSource tlfSource = new TLFDataSource(inputPath, gradoopConfig);
    GraphTransactions graphs = tlfSource.getGraphTransactions();

    // set config
    FSMConfig fsmConfig = new FSMConfig(
      Float.parseFloat(cmd.getOptionValue(TFSMParam.m.toString())),
      Boolean.parseBoolean(cmd.getOptionValue(TFSMParam.d.toString())),
      1,
      14,
      Boolean.parseBoolean(cmd.getOptionValue(TFSMParam.r.toString())),
      CanonicalLabel.valueOf(cmd.getOptionValue(TFSMParam.n.toString())),
      FilterStrategy.valueOf(cmd.getOptionValue(TFSMParam.f.toString())),
      GrowthStrategy.valueOf(cmd.getOptionValue(TFSMParam.g.toString())),
      IterationStrategy.valueOf(cmd.getOptionValue(TFSMParam.t.toString()))
    );

    // mine
    GraphTransactions frequentSubgraphs =
      new TransactionalFSM(fsmConfig).execute(graphs);

    // write statistics
    writeCSV(cmd.iterator(), frequentSubgraphs.getTransactions().count());
  }

  /**
   * Method to create and add lines to a csv-file
   * @throws IOException
   * @param iterator
   * @param count
   */
  private static void writeCSV(Iterator<Option> iterator, long count) throws
    IOException {

    List<String> columns = Lists.newArrayList();

    String logPath = "";

    while (iterator.hasNext()) {
      Option op = iterator.next();

      if (op.getArgName().equals(TFSMParam.l.toString())) {
        logPath = op.getValue();
      } else {
        columns.add(op.getValue());
      }
    }

    columns.add(String.valueOf(count));

    columns.add(String.valueOf(getExecutionEnvironment()
      .getLastJobExecutionResult()
      .getNetRuntime(TimeUnit.SECONDS)));

    File f = new File(logPath);
    String row = StringUtils.join(columns, "|");

    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, row, true);
    } else {
      PrintWriter writer = new PrintWriter(logPath, "UTF-8");
      writer.print(row);
      writer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return TFSMBenchmark.class.getName();
  }
}
