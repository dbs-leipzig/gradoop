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

package org.gradoop.examples.nestedmodel;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.examples.grouping.GroupingRunner;
import org.gradoop.examples.nestedmodel.benchmarks.BenchmarkResult;
import org.gradoop.examples.nestedmodel.benchmarks.Time;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.reader.parsers.rawedges.NumberTokenizer;
import org.gradoop.flink.io.reader.parsers.rawedges.RawEdgeFileParser;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.collect.Collect;
import org.gradoop.flink.model.impl.nested.operators.nesting.Nesting;
import org.gradoop.flink.model.impl.nested.operators.random.RandomSample;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

/**
 * Benchmarks the nested model by nesting with samples of the main subgraph.
 *
 * This other implementation takes as an input the elements obtained by reading the graph
 * source.
 */
public class BenchmarkNestWithActualNesting extends AbstractRunner implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_GROUP_PATH = "g";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_GROUP_PATH, "groups", true,
      "Path to group file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
  }


  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, GroupingRunner.class.getName());
    if (cmd == null) {
      return;
    }

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    final String groupsPath = cmd.getOptionValue(OPTION_GROUP_PATH);

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // load graph from edge list
    RawEdgeFileParser files = new RawEdgeFileParser();
    files.fromFile(inputPath);
    files.setEnvironment(env);
    GraphDataSource<String> gds = files.getDataset(false, conf);

    // read logical graph
    LogicalGraph logicalGraph = gds.getLogicalGraph();

    // Reading the file from
    NumberTokenizer tokenizer = new NumberTokenizer();


    // Output file
    BenchmarkResult toCSV = new BenchmarkResult();
    File fout = new File(outputPath);
    FileOutputStream fos = new FileOutputStream(fout);
    BufferedWriter csv = new BufferedWriter(new OutputStreamWriter(fos));
    csv.write(toCSV.getHeader());
    csv.newLine();
    toCSV.setNestingOperandNumberOfElements(10);

    toCSV.setDatasetSize(Integer.MAX_VALUE);

    for (long size = 10; size < 1000000; size = size * 10) {
      System.out.println("Size: " + size);
      toCSV.setNestingOperandSizeForEachElement(size);
      for (int times = 0; times < 10; times++) {
        System.out.println("\tTime: " + times);
        // Where to run the operations
        DataLake dl = new DataLake(logicalGraph);
        Collect collector = new Collect(dl.asNormalizedGraph().getConfig());
        IdGraphDatabase leftOperand = dl.getIdDatabase();
        Nesting n = new Nesting();
        for (int i = 0; i < 10; i++) {
          RandomSample s = new RandomSample(GradoopId.get(), size, i);
          collector.add(dl.run(s).with(leftOperand));
        }
        IdGraphDatabase rightOperand = collector.asIdGraphDatabase();
        Time t = Time.milliseconds();
        IdGraphDatabase resultGdb = dl.run(n).with(leftOperand, rightOperand);
        /*
        TODO: best solution to check the reading of all the elements
          * no secondary memory writing as in WriteCSV
          * only check the time required to create one element, without allocating the
            result in main memory
        try {
          resultGdb.getGraphHeads().collect();
          resultGdb.getGraphHeadToVertex().collect();
          resultGdb.getGraphHeadToEdge().collect();
        } catch (Exception e) {

        }
        */
        Time result = Time.milliseconds().difference(t);
        toCSV.setUnit(result.getRepresentation(), result.getTime());
        csv.write(toCSV.valueRowToCSV());
        csv.newLine();
      }
    }

    csv.close();
    fos.close();

  }

  @Override
  public String getDescription() {
    return BenchmarkNestWithActualNesting.class.getName();
  }
}
