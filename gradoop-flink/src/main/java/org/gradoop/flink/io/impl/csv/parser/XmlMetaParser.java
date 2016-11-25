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

package org.gradoop.flink.io.impl.csv.parser;

import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.ObjectFactory;
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

/**
 * Parser for XML meta information.
 */
public class XmlMetaParser {

  /**
   * Parses content from the xml file which satisfies the schema and returns the datasource object.
   *
   * @param xsdSchema path to the xml schema
   * @param xmlDatei path to the xml file
   * @return datasource which has a name and contains csv objects
   *
   * @throws SAXException schema loading failed
   * @throws JAXBException jaxb context failed
   * @throws IOException xml loading failed
   */
  public static Datasource parse(String xsdSchema, String xmlDatei)
    throws SAXException, JAXBException, IOException {

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = (xsdSchema == null || xsdSchema.trim().length() == 0) ?
      null : schemaFactory.newSchema(new File(xsdSchema));
    JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());

    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    unmarshaller.setSchema(schema);
    unmarshaller.setEventHandler(new XmlValidationEventHandler());

    FileInputStream fileInputStream = new FileInputStream(xmlDatei);
    Datasource source;
    try {
      source = (Datasource) unmarshaller.unmarshal(fileInputStream);

    } finally {
      fileInputStream.close();

    }

    return source;
  }

}
