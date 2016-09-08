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



import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;

public class CSVDataSource extends CSVBase implements DataSource {

  public static final String CACHED_FILE = "cachedfile";

  private String delimiter;


  public CSVDataSource(GradoopFlinkConfig config, String metaXmlPath,
    String delimiter) {
    super(config, metaXmlPath);
    this.delimiter = delimiter;

    ExecutionEnvironment env = config.getExecutionEnvironment();
    env.registerCachedFile(metaXmlPath, CACHED_FILE);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    env.registerCachedFile(getMetaXmlPath(), CACHED_FILE);

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
