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
package org.gradoop.flink.model.impl.operators.cypher.capf.result;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.AddGradoopIdToRow;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.AddNewGraphs;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.AggregateGraphs;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.CreateGraphHeadWithProperties;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.PropertyDecoder;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.functions.SplitRow;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.opencypher.flink.api.CAPFSession;
import org.opencypher.flink.impl.CAPFRecords;
import org.opencypher.okapi.api.graph.CypherResult;
import org.opencypher.okapi.ir.api.expr.Expr;
import org.opencypher.okapi.ir.api.expr.Var;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wrapper containing the results of a CAPF query.
 */
public class CAPFQueryResult {

  /**
   * Logical optimizer rules to be removed for better optimizer performance.
   *
   * {@code ProjectMergeRule:force_mode} would result in long optimization times.
   */
  private static final List<String> DISABLED_RULES =
    Collections.singletonList("ProjectMergeRule:force_mode");

  /**
   * The wrapped CAPFRecords.
   */
  private CAPFRecords records;

  /**
   * Flag, set true iff the CAPFResult contains graph entities and can be transformed into a graph.
   */
  private boolean isGraph;

  /**
   * The CAPFSession that was used to create the result.
   */
  private CAPFSession session;

  /**
   * Mapping between the long ids and the original vertices.
   */
  private DataSet<Tuple2<Long, Vertex>> verticesWithIds;

  /**
   * Mapping between the long ids and the original edges.
   */
  private DataSet<Tuple2<Long, Edge>> edgesWithIds;

  /**
   * The GradoopFlinkConfig.
   */
  private GradoopFlinkConfig config;

  /**
   * Constructor;
   *
   * @param result          result of CAPF query
   * @param verticesWithIds map between long id and original vertex
   * @param edgesWithIds    map between long id and original edge
   * @param config          the gradoop config
   */
  public CAPFQueryResult(
    CypherResult result,
    DataSet<Tuple2<Long, Vertex>> verticesWithIds,
    DataSet<Tuple2<Long, Edge>> edgesWithIds,
    GradoopFlinkConfig config) {
    this.records = (CAPFRecords) result.records();
    this.verticesWithIds = verticesWithIds;
    this.edgesWithIds = edgesWithIds;
    this.config = config;

    this.session = ((CAPFRecords) result.records()).capf();
    this.isGraph = !records.header().entityVars().isEmpty();
  }


  /**
   * Returns true iff the result contained entities that can be returned as graph collection.
   *
   * @return true iff results contain graphs
   */
  public boolean containsGraphs() {
    return isGraph;
  }

  /**
   * Get the graphs contained in the CAPF query result.
   * Returns null if the result contains no graphs.
   *
   * @return graphs contained in CAPF query iff there are any, else null
   */
  public GraphCollection getGraphs() {

    if (!isGraph) {
      return null;
    }

    Set<Var> nodeVars = new HashSet<>();
    Set<Var> relVars = new HashSet<>();
    Set<Var> otherVars = new HashSet<>();

    Iterator<Var> varIt = records.header().vars().iterator();
    while (varIt.hasNext()) {
      otherVars.add(varIt.next());
    }

    Iterator<Var> nodeVarIt = records.header().nodeEntities().iterator();
    while (nodeVarIt.hasNext()) {
      Var nodeVar = nodeVarIt.next();
      nodeVars.add(nodeVar);
      otherVars.remove(nodeVar);
    }

    Iterator<Var> relVarIt = records.header().relationshipEntities().iterator();
    while (relVarIt.hasNext()) {
      Var relVar = relVarIt.next();
      relVars.add(relVar);
      otherVars.remove(relVar);
    }

    StringBuilder entityFieldsBuilder = new StringBuilder();
    for (Var var : nodeVars) {
      entityFieldsBuilder.append(records.header().column((Expr) var)).append(",");
    }

    for (Var var : relVars) {
      entityFieldsBuilder.append(records.header().column((Expr) var)).append(",");
    }

    StringBuilder otherFieldsBuilder = new StringBuilder();
    List<String> otherVarNames = new ArrayList<>();

    for (Var var : otherVars) {
      otherVarNames.add(var.name());
      otherFieldsBuilder.append(
        records.header().getColumn((Expr) var).get()).append(", ");
    }

    String fieldString = entityFieldsBuilder.toString() + otherFieldsBuilder.toString();
    if (fieldString.length() > 0) {
      fieldString = fieldString.substring(0, fieldString.length() - 1);
    }

    TypeInformation<Row> rowTypeInfo = TypeInformation.of(Row.class);

    // Workaround for usable optimizer times
    removeSlowOptimizationRule();

    // entities, others, id
    org.apache.flink.api.scala.DataSet<Row> scalarowsWithNewIds = session.tableEnv()
      .toDataSet(records.table().table().select(fieldString), rowTypeInfo);

    DataSet<Row> rowsWithNewIds = scalarowsWithNewIds.javaSet()
      .map(new AddGradoopIdToRow());

    int entityFieldsCount = nodeVars.size() + relVars.size();
    int otherFieldsCount = otherVars.size();

    DataSet<GraphHead> graphHeads = rowsWithNewIds
      .map(new CreateGraphHeadWithProperties(
        entityFieldsCount,
        entityFieldsCount + otherFieldsCount,
        config.getGraphHeadFactory(),
        otherVarNames)
      );

    DataSet<Tuple2<Long, GradoopIdSet>> rowsWithGraphIdSets = rowsWithNewIds
      .flatMap(new SplitRow(0, entityFieldsCount))
      .groupBy(0)
      .reduceGroup(new AggregateGraphs<>());

    DataSet<Vertex> vertices =
      rowsWithGraphIdSets
        .join(verticesWithIds)
        .where(0)
        .equalTo(0)
        .with(new AddNewGraphs<>());

    DataSet<Edge> edges =
      rowsWithGraphIdSets
        .join(edgesWithIds)
        .where(0)
        .equalTo(0)
        .with(new AddNewGraphs<>());

    vertices = vertices.map(new PropertyDecoder<>());
    edges = edges.map(new PropertyDecoder<>());

    return config.getGraphCollectionFactory().fromDataSets(graphHeads, vertices, edges);
  }

  /**
   * Workaround to remove slow logical optimization rules listed in
   * {@link CAPFQueryResult#DISABLED_RULES}.
   *
   * This method accesses protected scala flink functions. See Issue #1221
   * (https://github.com/dbs-leipzig/gradoop/issues/1221).
   */
  private void removeSlowOptimizationRule() {
    List<RelOptRule> ruleList = new ArrayList<>();
    for (RelOptRule rule : session.tableEnv().getLogicalOptRuleSet()) {
      if (!DISABLED_RULES.contains(rule.toString())) {
        ruleList.add(rule);
      }
    }
    CalciteConfigBuilder builder = new CalciteConfigBuilder()
      .replaceLogicalOptRuleSet(RuleSets.ofList(ruleList));

    // rebuild old calcite config
    CalciteConfig calciteConfig = session.tableEnv().config().getCalciteConfig();
    if (calciteConfig.replacesDecoRuleSet()) {
      builder.replaceDecoRuleSet(calciteConfig.getDecoRuleSet().get());
    }
    if (calciteConfig.replacesNormRuleSet()) {
      builder.replaceNormRuleSet(calciteConfig.getNormRuleSet().get());
    }
    if (calciteConfig.replacesPhysicalOptRuleSet()) {
      builder.replacePhysicalOptRuleSet(calciteConfig.getPhysicalOptRuleSet().get());
    }
    if (calciteConfig.replacesSqlOperatorTable()) {
      builder.replaceSqlOperatorTable(calciteConfig.getSqlOperatorTable().get());
    }
    if (calciteConfig.getSqlParserConfig().isDefined()) {
      builder.replaceSqlParserConfig(calciteConfig.getSqlParserConfig().get());
    }
    if (calciteConfig.getSqlToRelConverterConfig().isDefined()) {
      builder.replaceSqlToRelConverterConfig(calciteConfig.getSqlToRelConverterConfig().get());
    }

    session.tableEnv().config().setCalciteConfig(builder.build());
  }

  /**
   * Get the flink table from the CAPF query result.
   *
   * @return flink table containing the CAPF query result
   */
  public Table getTable() {
    return records.table().table();
  }
}
