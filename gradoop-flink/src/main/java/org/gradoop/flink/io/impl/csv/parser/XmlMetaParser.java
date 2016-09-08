package org.gradoop.flink.io.impl.csv.parser;


import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;

public class XmlMetaParser {

  public static Datasource parse(String xsdSchema, String xmlDatei) throws
    MalformedURLException, SAXException, JAXBException, FileNotFoundException {

    SchemaFactory schemaFactory =
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema
      = (xsdSchema == null || xsdSchema.trim().length() == 0)
      ? null : schemaFactory.newSchema( new File( xsdSchema ) );
    JAXBContext jaxbContext =
      JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());

    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    unmarshaller.setSchema(schema);
    unmarshaller.setEventHandler(new XmlValidationEventHandler());

    Datasource source = (Datasource) unmarshaller.unmarshal(new
      FileInputStream(xmlDatei));

    return source;
  }

}
