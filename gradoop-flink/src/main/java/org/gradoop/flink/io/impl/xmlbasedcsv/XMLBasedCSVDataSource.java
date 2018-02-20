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

package org.gradoop.flink.io.impl.xmlbasedcsv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.CSVToContent;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.CSVToElement;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.CSVTypeFilter;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.EPGMElementToPojo;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.ElementToElementGraphKey;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.GradoopEdgeIds;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.GraphHeadToGraphKeyGraphHead;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.VertexIdsToMap;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.VertexToVertexIds;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.GraphKeyToGraphKeyNullGraphHead;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.DistinctGraphKeysWithHead;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.SetElementGraphIds;
import org.gradoop.flink.io.impl.xmlbasedcsv.functions.GraphHeadKeyMap;
import org.gradoop.flink.io.impl.xmlbasedcsv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.xmlbasedcsv.pojos.CsvExtension;
import org.gradoop.flink.io.impl.xmlbasedcsv.pojos.Datasource;
import org.gradoop.flink.io.impl.xmlbasedcsv.pojos.Domain;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Creates an EPGM instance from CSV files. Their format has to be defined
 * with a xml file. The schema for the xml is located at
 * {@link resources/data/xmlbasedcsv/csv_format.xsd}.
 */
public class XMLBasedCSVDataSource extends XMLBasedCSVBase implements DataSource {
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  private EdgeFactory edgeFactory;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param metaXmlPath xml file
   * @param csvDir csv directory
   * @param config Gradoop Flink configuration
   */
  public XMLBasedCSVDataSource(String metaXmlPath, String csvDir, GradoopFlinkConfig config) {
    super(metaXmlPath, csvDir, config);

    graphHeadFactory = config.getGraphHeadFactory();
    vertexFactory = config.getVertexFactory();
    edgeFactory = config.getEdgeFactory();
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    DataSet<GraphHead> graphHeads;
    DataSet<Vertex> vertices;
    DataSet<Edge> edges;

    // parse the xml file to a datasource and select each csv object
    Datasource datasource = null;
    try {
      datasource = XmlMetaParser.parse(getXsdPath(), getMetaXmlPath());
    } catch (SAXException | JAXBException e) {
      e.printStackTrace();
    }
    List<CsvExtension> csvList = Lists.newArrayList();
    if (datasource != null) {
      for (Domain domain : datasource.getDomain()) {
        for (CsvExtension csv : domain.getCsv()) {
          csv.setDatasourceName(datasource.getName());
          csv.setDomainName(domain.getName());
          csvList.add(csv);
        }
      }
    }

    // load the content for each csv file described in the xml file
    CsvExtension first = csvList.remove(0);
    DataSet<Tuple2<CsvExtension, String>> csvContent = env
      .readTextFile(getCsvDir() + first.getName())
      .map(new CSVToContent(first));
    for (CsvExtension csvFile : csvList) {
      csvContent = csvContent
        .union(env
          .readTextFile(getCsvDir() + csvFile.getName())
          .map(new CSVToContent(csvFile)));
    }

    // map each content line to an epgm element
    DataSet<EPGMElement> elements = csvContent
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory, edgeFactory));

    // get all the graph heads
    graphHeads = elements
      .filter(new CSVTypeFilter(GraphHead.class))
      .map(new EPGMElementToPojo<GraphHead>())
      .returns(graphHeadFactory.getType());

    // get all vertices
    vertices = elements
      .filter(new CSVTypeFilter(Vertex.class))
      .map(new EPGMElementToPojo<Vertex>())
      .returns(vertexFactory.getType());

    // create map from class key to gradoop id
    DataSet<Map<String, GradoopId>> vertexIds = vertices
      .map(new VertexToVertexIds())
      .reduceGroup(new VertexIdsToMap());

    // get all edges and adjust the vertex keys to their corresponding gradoop id
    edges = elements
      .filter(new CSVTypeFilter(Edge.class))
      .map(new EPGMElementToPojo<Edge>())
      .returns(edgeFactory.getType())
      .map(new GradoopEdgeIds())
      .withBroadcastSet(vertexIds, XMLBasedCSVConstants.BROADCAST_ID_MAP);

    // get all graph keys from vertex properties
    DataSet<Tuple2<Vertex, String>> vertexGraphKeys = vertices
      .flatMap(new ElementToElementGraphKey<Vertex>());

    // get all graph keys from edge properties
    DataSet<Tuple2<Edge, String>> edgeGraphKeys = edges
      .flatMap(new ElementToElementGraphKey<Edge>());

    // map each graphhead to its key from xml file
    DataSet<Tuple2<String, GraphHead>> keyGraphHead = graphHeads
      .map(new GraphHeadToGraphKeyGraphHead());

    // take all distinct graph keys which were read from or created for a vertex those are not yet
    // mapped to an existing graphhead
    keyGraphHead = keyGraphHead
      .union(vertexGraphKeys
          .map(new Value1Of2<Vertex, String>())
        .union(edgeGraphKeys
          .map(new Value1Of2<Edge, String>()))
        .distinct()
        .map(new GraphKeyToGraphKeyNullGraphHead()));

    // distinct(groupbBy+reduceGroup) with validation that only existing graphhead is used, if
    // there is one, otherwise a graphhead is created
    keyGraphHead = keyGraphHead
      .groupBy(0)
      .reduceGroup(new DistinctGraphKeysWithHead(graphHeadFactory));

    // all graphheads
    graphHeads = keyGraphHead.map(new Value1Of2<String, GraphHead>());
    // contains one map from graph key to gradoop id
    DataSet<Map<String, GradoopId>> graphHeadMap = graphHeads.reduceGroup(new GraphHeadKeyMap());

    // set all graph heads
    vertices = vertexGraphKeys
      .groupBy("f0.id")
      .reduceGroup(new SetElementGraphIds<Vertex>())
      .withBroadcastSet(graphHeadMap, XMLBasedCSVConstants.BROADCAST_GRAPHHEADS);

    // set all graph heads
    edges = edgeGraphKeys
      .groupBy("f0.id")
      .reduceGroup(new SetElementGraphIds<Edge>())
      .withBroadcastSet(graphHeadMap, XMLBasedCSVConstants.BROADCAST_GRAPHHEADS);

    return GraphCollection.fromDataSets(graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
