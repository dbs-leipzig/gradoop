/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.examples.grouping;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;

import java.util.Arrays;

/**
 * A dedicated program for parametrized graph grouping.
 */
public class GroupingRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Vertex grouping key option
   */
  public static final String OPTION_VERTEX_GROUPING_KEY = "vgk";
  /**
   * Edge grouping key option
   */
  public static final String OPTION_EDGE_GROUPING_KEY = "egk";
  /**
   * Use vertex label option
   */
  public static final String OPTION_USE_VERTEX_LABELS = "uvl";
  /**
   * Use edge label option
   */
  public static final String OPTION_USE_EDGE_LABELS = "uel";
  /**
   * Use local combine strategy for grouping
   */
  public static final String OPTION_GROUP_COMBINE = "gc";
  /**
   * Group edge centric
   */
  public static final String OPTION_GROUP_EDGE_CENTRIC = "gec";
  /**
   * Consider edges source for grouping
   */
  public static final String OPTION_USE_EDGE_SOURCE = "ues";
  /**
   * Consider edges target for grouping
   */
  public static final String OPTION_USE_EDGE_TARGET = "uet";
  /**
   * Label group for vertices
   */
  public static final String OPTION_VERTEX_LABEL_GROUP = "vlg";
  /**
   * Label group for edges
   */
  public static final String OPTION_EDGE_LABEL_GROUP = "elg";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
    OPTIONS.addOption(OPTION_VERTEX_GROUPING_KEY, "vertex-grouping-key", true,
      "EPGMProperty key to group vertices on.");
    OPTIONS.addOption(OPTION_EDGE_GROUPING_KEY, "edge-grouping-key", true,
      "EPGMProperty key to group edges on.");
    OPTIONS.addOption(OPTION_USE_VERTEX_LABELS, "use-vertex-labels", false,
      "Group on vertex labels");
    OPTIONS.addOption(OPTION_USE_EDGE_LABELS, "use-edge-labels", false,
      "Group on edge labels");
    OPTIONS.addOption(OPTION_GROUP_COMBINE, "group-combine", false,
      "Group with local combine");
    OPTIONS.addOption(OPTION_GROUP_EDGE_CENTRIC, "group-edge-centric", false,
      "Group edge centric");
    OPTIONS.addOption(OPTION_USE_EDGE_SOURCE, "use-edge-source", false,
      "Group on edge labels");
    OPTIONS.addOption(OPTION_USE_EDGE_TARGET, "use-edge-target", false,
      "Group on edge labels");
    OPTIONS.addOption(OPTION_VERTEX_LABEL_GROUP, "vertex-label-group", true,
      "Vertex label group: label%key%key-labelTwo%keyTwo");
    OPTIONS.addOption(OPTION_EDGE_LABEL_GROUP, "edge-label-group", true,
      "Edge label group: label%key%key-labelTwo%keyTwo");

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
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    boolean useVertexKey = cmd.hasOption(OPTION_VERTEX_GROUPING_KEY);
    String vertexKey =
      useVertexKey ? cmd.getOptionValue(OPTION_VERTEX_GROUPING_KEY) : null;
    boolean useEdgeKey = cmd.hasOption(OPTION_EDGE_GROUPING_KEY);
    String edgeKey =
      useEdgeKey ? cmd.getOptionValue(OPTION_EDGE_GROUPING_KEY) : null;
    boolean useVertexLabels = cmd.hasOption(OPTION_USE_VERTEX_LABELS);
    boolean useEdgeLabels = cmd.hasOption(OPTION_USE_EDGE_LABELS);

    boolean groupCombine = cmd.hasOption(OPTION_GROUP_COMBINE);
    boolean groupEdgeCentric = cmd.hasOption(OPTION_GROUP_EDGE_CENTRIC);
    boolean useEdgeSource = cmd.hasOption(OPTION_USE_EDGE_SOURCE);
    boolean useEdgeTarget = cmd.hasOption(OPTION_USE_EDGE_TARGET);

    String[] vertexLabelGroups = cmd.hasOption(OPTION_VERTEX_LABEL_GROUP) ?
      cmd.getOptionValue(OPTION_VERTEX_LABEL_GROUP).split("-") : null;
    String[] edgeLabelGroups = cmd.hasOption(OPTION_EDGE_LABEL_GROUP) ?
      cmd.getOptionValue(OPTION_EDGE_LABEL_GROUP).split("-") : null;

    // initialize EPGM database
    LogicalGraph graphDatabase = readLogicalGraph(inputPath);

    // initialize grouping method
    Grouping grouping = getOperator(groupEdgeCentric, groupCombine,
      vertexKey, edgeKey, useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups,
      useEdgeSource, useEdgeTarget);

    // call grouping on whole database graph
    LogicalGraph summarizedGraph = graphDatabase.callForGraph(grouping);

    if (summarizedGraph != null) {
      writeLogicalGraph(summarizedGraph, outputPath);
    } else {
      System.err.println("wrong parameter constellation");
    }
  }

  /**
   * Returns the grouping operator implementation based on the given strategy.
   *
   * @param groupEdgeCentric      group edge centric
   * @param groupCombine          use local combine
   * @param vertexKey             vertex property key used for grouping
   * @param edgeKey               edge property key used for grouping
   * @param useVertexLabels       use vertex label for grouping, true/false
   * @param useEdgeLabels         use edge label for grouping, true/false
   * @param useEdgeSource         consider edges source
   * @param useEdgeTarget         consider edges target
   * @param vertexLabelGroups     vertex label groups
   * @param edgeLabelGroups     edge label groups
   * @return grouping operator implementation
   */
  private static Grouping getOperator(boolean groupEdgeCentric, boolean groupCombine,
    String vertexKey, String edgeKey, boolean useVertexLabels, boolean useEdgeLabels,
    String[] vertexLabelGroups, String[] edgeLabelGroups,
    boolean useEdgeSource, boolean useEdgeTarget) {
    GroupingStrategy groupingStrategy = groupCombine ? GroupingStrategy.GROUP_COMBINE :
      GroupingStrategy.GROUP_REDUCE;
    GroupingStrategy centricalStrategy = groupEdgeCentric ? GroupingStrategy.EDGE_CENTRIC :
      GroupingStrategy.VERTEX_CENTRIC;

    Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder()
      .setCentricalStrategy(centricalStrategy)
      .setStrategy(groupingStrategy)
      .useVertexLabel(useVertexLabels)
      .useEdgeLabel(useEdgeLabels)
      .addGlobalVertexAggregator(new CountAggregator())
      .addGlobalEdgeAggregator(new CountAggregator())
      .useEdgeSource(useEdgeSource)
      .useEdgeTarget(useEdgeTarget);

    if (vertexKey != null) {
      builder.addVertexGroupingKey(vertexKey);
    }
    if (edgeKey != null) {
      builder.addEdgeGroupingKey(edgeKey);
    }
    if (vertexLabelGroups != null && vertexLabelGroups.length > 0) {
      String[] vertexLabelGroupitems;
      for (String vertexLabelGroup : vertexLabelGroups) {
        vertexLabelGroupitems = vertexLabelGroup.split("%");
        builder.addVertexLabelGroup(vertexLabelGroupitems[0], Arrays.asList(
          Arrays.copyOfRange(vertexLabelGroupitems, 1, vertexLabelGroupitems.length)));
      }
    }
    if (edgeLabelGroups != null && edgeLabelGroups.length > 0) {
      String[] edgeLabelGroupitems;
      for (String edgeLabelGroup : edgeLabelGroups) {
        edgeLabelGroupitems = edgeLabelGroup.split("%");
        builder.addEdgeLabelGroup(edgeLabelGroupitems[0], Arrays.asList(
          Arrays.copyOfRange(edgeLabelGroupitems, 1, edgeLabelGroupitems.length)));
      }
    }

    return builder
      .build();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_GROUP_EDGE_CENTRIC)) {
      if (!cmd.hasOption(OPTION_VERTEX_GROUPING_KEY) &&
        !cmd.hasOption(OPTION_USE_VERTEX_LABELS) &&
        !cmd.hasOption(OPTION_VERTEX_LABEL_GROUP)) {
        throw new IllegalArgumentException(
          "Chose at least a vertex grouping key or use vertex labels or add a vertex label group.");
      }
    } else {
      if (!cmd.hasOption(OPTION_EDGE_GROUPING_KEY) &&
        !cmd.hasOption(OPTION_USE_EDGE_LABELS) &&
        !cmd.hasOption(OPTION_EDGE_LABEL_GROUP)) {
        throw new IllegalArgumentException(
          "Chose at least a vertex grouping key or use vertex labels or add a vertex label group.");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return GroupingRunner.class.getName();
  }
}
