/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmark.cypher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized graph cypher benchmark.
 */
public class CAPFBenchmark extends AbstractRunner implements ProgramDescription {
  /**
   * Option for used first name in query.
   */
  private static final String OPTION_FIRST_NAME = "n";
  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "o";
  /**
   * Option to use a custom query string
   */
  private static final String OPTION_CUSTOM_QUERY = "cq";
  /**
   * Option to use a predefined query string
   */
  private static final String OPTION_PREDEFINED_QUERY = "pq";
  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used output path for csv statistics
   */
  private static String CSV_PATH;
  /**
   * Used query
   */
  private static String QUERY;
  /**
   * Used first name for query (q1,q2,q3)
   */
  private static String FIRST_NAME;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Input path to indexed source files.");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Output path to csv statistics output");
    OPTIONS.addOption(OPTION_CUSTOM_QUERY, "customQuery", true,
      "Custom query specified by user");
    OPTIONS.addOption(OPTION_PREDEFINED_QUERY, "predefinedQuery", true,
      "Predefined cypher query");
    OPTIONS.addOption(OPTION_FIRST_NAME, "firstName", true,
      "Used first Name in Cypher Query");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception IO or execution Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, CAPFBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    // read graph
    LogicalGraph graph = readLogicalGraph(INPUT_PATH);

    // prepare collection
    GraphCollection collection;

    System.out.println(QUERY);
    // execute cypher with or without statistics
    CAPFQueryResult result = graph.cypher(QUERY);


    if (result.containsGraphs()) {
      collection = result.getGraphs();
      List<Vertex> vertices = new ArrayList<>();
      collection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
      System.out.println(collection.getConfig().getExecutionEnvironment().getExecutionPlan());
//      System.out.println(collection.getGraphHeads().count());
    } else {
      result.getTable().printSchema();
    }

    // execute and write job statistics
    writeCSV(graph.getConfig().getExecutionEnvironment());
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    FIRST_NAME = cmd.getOptionValue(OPTION_FIRST_NAME);

    if (cmd.hasOption(OPTION_CUSTOM_QUERY)) {
      StringBuilder queryBuilder = new StringBuilder();
      for (String val : cmd.getOptionValues(OPTION_CUSTOM_QUERY)) {
        queryBuilder.append(val).append(" ");
      }
      QUERY = queryBuilder.toString();
    } else if (cmd.hasOption(OPTION_PREDEFINED_QUERY)) {
      QUERY = getPredefinedQuery(cmd.getOptionValue(OPTION_PREDEFINED_QUERY));
    }
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!(cmd.hasOption(OPTION_PREDEFINED_QUERY) || cmd.hasOption(OPTION_CUSTOM_QUERY))) {
      throw new IllegalArgumentException("Define a query to run.");
    }
    if (cmd.hasOption(OPTION_PREDEFINED_QUERY) && cmd.hasOption(OPTION_CUSTOM_QUERY)) {
      throw new IllegalArgumentException("Specify either a custom or predefined query, not both!");
    }
    if (cmd.hasOption(OPTION_PREDEFINED_QUERY)) {
      String predefQuery = cmd.getOptionValue(OPTION_PREDEFINED_QUERY);
      if (predefQuery.equals("q1") || predefQuery.equals("q2") || predefQuery.equals("q3")) {
        if (!cmd.hasOption(OPTION_FIRST_NAME)) {
          throw new IllegalArgumentException("Queries 1, 2 and 3 require a first name.");
        }
      }
    }
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String.format("%s|%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "query",
      "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
      QUERY,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  /**
   * Returns used query for benchmark
   *
   * @param query argument input
   * @return used query
   */
  private static String getPredefinedQuery(String query) {
    switch (query) {
    case "q1":
      return CAPFQueries.q1(FIRST_NAME);
    case "q2":
      return CAPFQueries.q2(FIRST_NAME);
    case "q3":
      return CAPFQueries.q3(FIRST_NAME);
    case "q4":
      return CAPFQueries.q4();
    case "q5":
      return CAPFQueries.q5();
    case "q6":
      return CAPFQueries.q6();
    default:
      throw new IllegalArgumentException("Unsupported query: " + query);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return CAPFBenchmark.class.getName();
  }
}
