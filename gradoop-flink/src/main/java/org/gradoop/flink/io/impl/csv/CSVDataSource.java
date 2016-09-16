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

package org.gradoop.flink.io.impl.csv;



import org.apache.hadoop.io.Text;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVToVertex;
import org.gradoop.flink.io.impl.csv.functions.CsvTypeFilter;
import org.gradoop.flink.io.impl.csv.functions.XmlToGraphhead;
import org.gradoop.flink.io.impl.csv.parser.ObjectFactory;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class CSVDataSource extends CSVBase implements DataSource {

  public static final String CACHED_FILE = "cachedfile";

  private ExecutionEnvironment env;

  private String hdfsPrefix;


  public CSVDataSource(GradoopFlinkConfig config, String metaXmlPath,
    String hdfsPrefix) {
    super(config, metaXmlPath);
    this.hdfsPrefix = hdfsPrefix;

    env = getConfig().getExecutionEnvironment();
    env.registerCachedFile(getMetaXmlPath(), CACHED_FILE);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {

    Datasource source = null;

    SchemaFactory schemaFactory =
      SchemaFactory.newInstance( XMLConstants.W3C_XML_SCHEMA_NS_URI );
    Schema schema = null;
    try {
      schema = schemaFactory.newSchema( new File( getXsdPath() ) );
      JAXBContext jaxbContext =
        JAXBContext.newInstance(ObjectFactory.class.getPackage().getName() );
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      unmarshaller.setSchema( schema );
      source = (Datasource) unmarshaller.unmarshal(new FileInputStream(getMetaXmlPath()));

    } catch (SAXException e) {
      e.printStackTrace();
    } catch (JAXBException e) {
      e.printStackTrace();
    }

    DataSet<Datasource> datasource = env.fromElements(source);


    DataSet<Csv> csvGraphHead = datasource
      .filter(new CsvTypeFilter())
      .map(); //csv graphheads etc

    DataSet<GraphHead> graphHead = csvGraphHead
      .map(new XmlToGraphhead(env)); //import graphheads









    DataSet<Vertex> vertices =  getConfig().getExecutionEnvironment()
      .readHadoopFile(new TextInputFormat(),
      LongWritable.class, Text.class, getMetaXmlPath())
      .map(new CSVToVertex(getConfig().getVertexFactory()));


    return null;
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return null;
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return null;
  }
}
