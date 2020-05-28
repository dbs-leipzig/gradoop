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
package org.gradoop.examples.keyedgrouping;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.examples.keyedgrouping.functions.AgeRoundedTo10;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificKeyFunction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys.label;
import static org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys.labelSpecific;
import static org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys.property;

/**
 * A self-contained example on how to use {@link KeyedGrouping} and on how to define and use a custom
 * key-function.
 */
public class KeyedGroupingExample {

  /**
   * Runs the program on the example data graph.
   * <p>
   * This provides an example on how to use the {@link KeyedGrouping} operator with label-specific grouping
   * as well as a custom (user-defined) key-function.
   * <p>
   * Using the social network graph {@link SocialNetworkGraph}, the program will:
   * <ol>
   *   <li>load the graph from the given gdl string</li>
   *   <li>group the graph based on<ul>
   *     <li>vertices and edges by label</li>
   *     <li>vertices with label {@code Person} additionally by the rounded {@code birthday} property</li>
   *     <li>vertices with label {@code Forum} additionally by the {@code title} property</li>
   *   </ul></li>
   *   <li>print the resulting graph</li>
   * </ol>
   *
   * @param args arguments (unused)
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(SocialNetworkGraph.getGraphGDLString());

    // load the graph
    LogicalGraph graph = loader.getLogicalGraph();

    // define the key functions to use
    Map<String, List<KeyFunctionWithDefaultValue<EPGMVertex, ?>>> labelSpecificKeys = new HashMap<>();
    // note that you may use any java.util.List type, we are using singletonList here since we only use
    // one key function at a time
    labelSpecificKeys.put("Person", singletonList(new AgeRoundedTo10<>()));
    labelSpecificKeys.put("Forum", singletonList(property("title")));
    labelSpecificKeys.put(LabelSpecificKeyFunction.DEFAULT_GROUP_LABEL, singletonList(label()));
    List<KeyFunction<EPGMVertex, ?>> vertexKeys = singletonList(labelSpecific(labelSpecificKeys));
    List<KeyFunction<EPGMEdge, ?>> edgeKeys = singletonList(label());
    List<AggregateFunction> vertexAggregateFunctions = singletonList(new Count());
    List<AggregateFunction> edgeAggregateFunctions = singletonList(new Count());

    // initialize and call the operator.
    LogicalGraph grouped = graph.callForGraph(new KeyedGrouping<>(
      vertexKeys, vertexAggregateFunctions, edgeKeys, edgeAggregateFunctions));

    // print results
    grouped.print();
  }
}
