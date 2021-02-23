#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package ${package};

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Skeleton of a Gradoop application.
 * <p>
 * This is the main class of the application that will be executed by Flink.
 */
public class GradoopApplication {

  /**
   * The main entry point of the Gradoop application. This method is called by Flink and is used to declare
   * the workflow.
   *
   * @throws Exception on failure (this is usually required since {@code execute()} throws {@link Exception}s)
   */
  public static void main(String[] args) throws Exception {
    // Initialize Flink's execution environment.
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Initialize the Gradoop config. It is used to create and read graphs and graph elements.
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // Once the config is created, graphs and graph collections may be read using the various data sources.

    /*
    // Read the graph.
    DataSource source = new CSVDataSource("hdfs:///path/to/your/graph", config);
    LogicalGraph graph = source.getLogicalGraph();
    // Call some operators on the graph.
    LogicalGraph result = graph.callForGraph(new SomeOperator())
      ...;

    // Write the resulting graph to a data sink.
    DataSink sink = new CSVDataSink("hdfs:///path/to/the/result/graph", config);
    result.writeTo(sink, false);
    */

    // Execute the program. Creating Gradoop data sources, operators and data sinks implicitly creates a Flink workflow.
    // Call execute will run that workflow. Note that some operations like .print() also call execute.
    env.execute();
  }
}
