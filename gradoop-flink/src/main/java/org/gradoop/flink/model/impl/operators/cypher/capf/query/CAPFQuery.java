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
package org.gradoop.flink.model.impl.operators.cypher.capf.query;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.model.api.operators.Operator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.EdgeLabelFilter;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.EdgeToTuple;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.IdOfF1;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.PropertyEncoder;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.ReplaceSourceId;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.ReplaceTargetId;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.TupleToRow;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.UniqueIdWithOffset;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.VertexLabelFilter;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.functions.VertexToRow;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.opencypher.flink.api.CAPFSession;
import org.opencypher.flink.api.CAPFSession$;
import org.opencypher.flink.api.io.CAPFNodeTable;
import org.opencypher.flink.api.io.CAPFRelationshipTable;
import org.opencypher.flink.impl.table.FlinkCypherTable;
import org.opencypher.okapi.api.graph.PropertyGraph;
import org.opencypher.okapi.api.io.conversion.NodeMapping;
import org.opencypher.okapi.api.io.conversion.RelationshipMapping;
import org.opencypher.okapi.relational.api.io.EntityTable;
import scala.collection.JavaConversions;
import scala.collection.mutable.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.EDGE_ID;
import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.END_NODE;
import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.NODE_ID;
import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.OFFSET;
import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.PROPERTY_PREFIX;
import static org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQueryConstants.START_NODE;


/**
 * Execute a cypher query on a LogicalGraph via the CAPF (Cypher for Apache Flink)
 * API.
 */
public class CAPFQuery implements Operator {

  /**
   * The query string.
   */
  private String query;

  /**
   * MetaData object
   */
  private MetaData metaData;

  /**
   * The CAPF session the query will be executed in.
   */
  private CAPFSession session;

  /**
   * The number of vertices by label, ordered alphabetically.
   */
  private DataSet<Long> vertexCount;

  /**
   * Mapping between the long ids and the original vertices.
   */
  private DataSet<Tuple2<Long, Vertex>> verticesWithIds;

  /**
   * Mapping between the long ids and the original edges.
   */
  private DataSet<Tuple2<Long, Edge>> edgesWithIds;

  /**
   * Constructor
   *
   * @param query the query string
   * @param env   the execution environment
   */
  public CAPFQuery(String query, ExecutionEnvironment env) {

    this.query = query;

    this.vertexCount = null;
    this.session = CAPFSession$.MODULE$.create(
      new org.apache.flink.api.scala.ExecutionEnvironment(env)
    );
  }

  /**
   * Constructor
   *
   * @param query    the query string
   * @param metaData metaData object
   * @param env      the execution environment
   */
  public CAPFQuery(
    String query, MetaData metaData, ExecutionEnvironment env) {
    this.query = query;
    this.metaData = metaData;
    this.vertexCount = null;
    this.session = CAPFSession$.MODULE$.create(
      new org.apache.flink.api.scala.ExecutionEnvironment(env));
  }

  /**
   * Execute a cypher query on a given graph via the CAPF API.
   *
   * @param graph the graph that the query shall be executed on
   * @return the result of the query, either a graph collection or a flink table
   * @throws Exception if the execution or IO fails.
   */
  public CAPFQueryResult execute(LogicalGraph graph) throws Exception {

    if (metaData == null) {
      graph = transformGraphProperties(graph);
      metaData = new CSVMetaDataSource()
        .fromTuples(new CSVMetaDataSource().tuplesFromGraph(graph).collect());
    }
    // create flink tables of nodes as required by CAPF
    List<CAPFNodeTable> nodeTables = createNodeTables(graph);

    // create flink tables of relationships as required by CAPF
    List<CAPFRelationshipTable> relTables = createRelationshipTables(graph);

    // if there are no nodes, no edges can exit either, so we can terminate early
    if (nodeTables.size() > 0) {
      List<EntityTable<FlinkCypherTable.FlinkTable>> tables = new ArrayList<>(
        nodeTables.subList(1, nodeTables.size()));
      tables.addAll(relTables);

      Seq<EntityTable<FlinkCypherTable.FlinkTable>> tableSeq =
        JavaConversions.asScalaBuffer(tables);

      PropertyGraph g = session.readFrom(nodeTables.get(0), tableSeq);

      // construct a CAPFQueryResult from the CAPFResult returned by CAPF
      return new CAPFQueryResult(
        g.cypher(
          query,
          g.cypher$default$2(),
          g.cypher$default$3(),
          g.cypher$default$4()
        ),
        verticesWithIds,
        edgesWithIds,
        graph.getConfig()
      );
    }

    return null;
  }

  /**
   * Transform vertex and edge properties with types not yet supported by CAPF into string
   * representations.
   *
   * @param graph the graph
   * @return a graph with transformed vertex and edge properties
   */
  private LogicalGraph transformGraphProperties(LogicalGraph graph) {
    DataSet<Vertex> transformedVertices = graph.getVertices()
      .map(new PropertyEncoder<>());
    DataSet<Edge> transformedEdges = graph.getEdges()
      .map(new PropertyEncoder<>());

    return graph.getFactory().fromDataSets(transformedVertices, transformedEdges);
  }

  /**
   * Method to transform a DataSet of vertices into the flink table format
   * required by CAPF: Unique long ids for each vertex, one table per
   * vertex label and each property in a unique row field.
   *
   * @param graph the graph whose vertices should be transformed into CAPF tables
   * @return a list of node tables, one table per vertex label
   */
  private List<CAPFNodeTable> createNodeTables(LogicalGraph graph) {
    List<CAPFNodeTable> nodeTables = new ArrayList<>();

    verticesWithIds = graph.getVertices().map(new UniqueIdWithOffset<>());
    vertexCount = Count.count(graph.getVertices());

    // construct a table for each vertex label
    for (String label : metaData.getVertexLabels()) {
      List<PropertyMetaData> propertyTypes = metaData.getVertexPropertyMetaData(label);

      // list of all row field types
      TypeInformation<?>[] types = new TypeInformation<?>[propertyTypes.size() + 1];
      List<String> propKeys = new ArrayList<>(propertyTypes.size());

      // first field is long id
      types[0] = TypeInformation.of(Long.class);

      for (int i = 0; i < propertyTypes.size(); i++) {
        PropertyMetaData pmd = propertyTypes.get(i);
        propKeys.add(pmd.getKey());
        types[i + 1] = TypeInformation.of(MetaData.getClassFromTypeString(pmd.getTypeString()));
      }

      RowTypeInfo info = new RowTypeInfo(types);

      // zip all vertices of one label with a globally unique id
      DataSet<Tuple2<Long, Vertex>> verticesByLabelWithIds =
        verticesWithIds.filter(new VertexLabelFilter(label));

      // map vertices to row and wrap in scala DataSet
      org.apache.flink.api.scala.DataSet<Row> scalaRowDataSet =
        new org.apache.flink.api.scala.DataSet<>(
          verticesByLabelWithIds.map(new VertexToRow(propKeys)).returns(info),
          ClassTag$.MODULE$.apply(Row.class)
        );

      // build table schema string, naming each field in the table
      StringBuilder schemaStringBuilder = new StringBuilder(NODE_ID);
      NodeMapping nodeMapping = NodeMapping.withSourceIdKey(NODE_ID)
        .withImpliedLabel(label);

      for (String propKey : propKeys) {
        schemaStringBuilder.append(", ").append(PROPERTY_PREFIX).append(propKey);

        nodeMapping = nodeMapping.withPropertyKey(propKey, PROPERTY_PREFIX + propKey);
      }

      String schemaString = schemaStringBuilder.toString();

      // create table, add to node table list
      Table vertexTable = session.tableEnv()
        .fromDataSet(scalaRowDataSet).as(schemaString);

      nodeTables.add(CAPFNodeTable.fromMapping(nodeMapping, vertexTable));
    }

    return nodeTables;
  }

  /**
   * Method to transform a DataSet of edges into the flink table format
   * required by CAPF: Unique long ids for each edge, source and target are long ids,
   * one table per edge label and each property in a unique row field.
   *
   * @param graph the graph whose edges should be transformed into CAPF tables
   * @return a list of edge tables, one table per edge label
   */
  private List<CAPFRelationshipTable> createRelationshipTables(LogicalGraph graph) {
    List<CAPFRelationshipTable> relTables = new ArrayList<>();

    edgesWithIds = graph.getEdges().map(new UniqueIdWithOffset<>())
      .withBroadcastSet(vertexCount, OFFSET);

    // replace source and target with long ids
    DataSet<Tuple5<Long, Long, Long, String, Properties>> edgeTuples = edgesWithIds
      .map(new EdgeToTuple())
      .join(verticesWithIds)
      .where(1).equalTo(new IdOfF1<>()).with(new ReplaceSourceId())
      .join(verticesWithIds)
      .where(2).equalTo(new IdOfF1<>()).with(new ReplaceTargetId());

    // construct a table for each edge label
    for (String label : metaData.getEdgeLabels()) {
      List<PropertyMetaData> propertyTypes = metaData.getEdgePropertyMetaData(label);

      // list of all row field types
      TypeInformation<?>[] types = new TypeInformation<?>[propertyTypes.size() + 3];
      List<String> propKeys = new ArrayList<>(propertyTypes.size());

      // first fields are id, source id and target id
      types[0] = TypeInformation.of(Long.class); // id
      types[1] = TypeInformation.of(Long.class); // source
      types[2] = TypeInformation.of(Long.class); // target

      // other fields are properties
      for (int i = 0; i < propertyTypes.size(); i++) {
        PropertyMetaData pmd = propertyTypes.get(i);
        propKeys.add(pmd.getKey());
        types[i + 3] = TypeInformation.of(MetaData.getClassFromTypeString(pmd.getTypeString()));
      }

      RowTypeInfo info = new RowTypeInfo(types);

      // zip all edges of one label with a globally unique id
      DataSet<Tuple5<Long, Long, Long, String, Properties>> edgesByLabel = edgeTuples
        .filter(new EdgeLabelFilter(label));

      // map vertices to row and wrap in scala DataSet
      org.apache.flink.api.scala.DataSet<Row> scalaRowDataSet =
        new org.apache.flink.api.scala.DataSet<>(
          edgesByLabel.map(new TupleToRow(propKeys)).returns(info),
          ClassTag$.MODULE$.apply(Row.class)
        );

      // build table schema string, naming each field in the table
      StringBuilder schemaStringBuilder = new StringBuilder();
      schemaStringBuilder
        .append(EDGE_ID).append(", ")
        .append(START_NODE).append(", ")
        .append(END_NODE);

      RelationshipMapping relMapping = RelationshipMapping.withSourceIdKey(EDGE_ID)
        .withSourceStartNodeKey(START_NODE)
        .withSourceEndNodeKey(END_NODE)
        .withRelType(label);

      for (String propKey : propKeys) {
        schemaStringBuilder.append(", ").append(PROPERTY_PREFIX).append(propKey);
        relMapping = relMapping.withPropertyKey(propKey, PROPERTY_PREFIX + propKey);
      }

      String schemaString = schemaStringBuilder.toString();

      // create table, add to relationship table list
      Table edgeTable = session.tableEnv()
        .fromDataSet(scalaRowDataSet).as(schemaString);

      relTables.add(CAPFRelationshipTable.fromMapping(relMapping, edgeTable));
    }

    return relTables;
  }
}
