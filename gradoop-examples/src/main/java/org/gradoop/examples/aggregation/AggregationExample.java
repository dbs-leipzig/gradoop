/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.examples.aggregation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the aggregate operator on Gradoop's LogicalGraph class.
 *
 * The example uses the graph in dev-support/social-network.pdf
 */
public class AggregationExample {

  /**
   * Path to the example data graph
   */
  static private final String EXAMPLE_DATA_FILE = AggregationExample.class
    .getResource("/data/gdl/sna.gdl").getFile();

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the aggregate() method.
   * It both showcases the application of aggregation functions that are already provided by
   * Gradoop, as well as the definition of custom ones.
   * Documentation for all available aggregation functions as well as a detailed description of the
   * aggregate method can be found in the projects wiki.
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   *   Gradoop Wiki</a>
   *
   * Using the social network graph in the resources directory, the program will:
   * 1. extract a subgraph only containing vertices which are labeled "person"
   * 2. concatenate the values of the vertex property "name" of each vertex in the subgraph
   * 3. count the amount of vertices in the subgraph (aka the amount of persons)
   * 4. sum up the values of the vertex property "birthday"
   * 5. add the property "meanAge" to the graph head using a graph head transformation
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromFile(EXAMPLE_DATA_FILE);

    // get LogicalGraph representation of the social network graph
    LogicalGraph networkGraph = loader.getDatabase().getDatabaseGraph();

    // execute aggregations and graph-head transformations
    LogicalGraph result = networkGraph
      // extract subgraph with edges and vertices which are of interest
      .subgraph(v -> v.getLabel().equals("Person"), e -> true)
      // apply custom VertexAggregateFunctions
      .aggregate(new VertexAggregateFunction() {
        @Override
        public PropertyValue getVertexIncrement(Vertex vertex) {
          return PropertyValue.create(vertex.getPropertyValue("name").toString());
        }

        @Override
        public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
          aggregate.setString(aggregate.getString() + "," + increment.getString());
          return aggregate;
        }

        @Override
        public String getAggregatePropertyKey() {
          return "list_of_names";
        }
      })
      // aggregate sum of vertices in order to obtain total amount of persons
      .aggregate(new VertexCount())
      // sum up values of the vertex property "birthday"
      .aggregate(new SumVertexProperty("birthday"))
      // add computed property "meanAge" to the graph head
      .transformGraphHead((TransformationFunction<GraphHead>) (current, transformed) -> {
        int sumAge = current.getPropertyValue("sum_birthday").getInt();
        Long numPerson = current.getPropertyValue("vertexCount").getLong();
        current.setProperty("meanAge", (double) sumAge / numPerson);
        return current;
      });

    // print graph head, which now contains the newly aggregated properties
    System.out.println(result.getGraphHead().collect());
  }
}
