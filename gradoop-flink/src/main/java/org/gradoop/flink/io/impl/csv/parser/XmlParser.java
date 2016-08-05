package org.gradoop.flink.io.impl.csv.parser;


import com.sun.org.apache.xerces.internal.impl.xs.XSImplementationImpl;
import com.sun.org.apache.xerces.internal.xs.XSLoader;
import com.sun.org.apache.xerces.internal.xs.XSModel;
import org.gradoop.flink.io.impl.csv.pojo.CSVFile;
import org.gradoop.flink.io.impl.csv.pojo.DataSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class XmlParser {

//  System.setProperty(DOMImplementationRegistry.PROPERTY, "com.sun.org.apache.xerces.internal.dom.DOMXSImplementationSourceImpl");
//  DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
//  com.sun.org.apache.xerces.internal.impl.xs.XSImplementationImpl impl = (XSImplementationImpl) registry.getDOMImplementation("XS-Loader");
//  XSLoader schemaLoader = impl.createXSLoader(null);
//  XSModel model = schemaLoader.loadURI("src/test/resources/my.xsd");


  public static CSVFile parseJAXB2(String xsdSchema, String xmlDatei) throws
    MalformedURLException, SAXException, JAXBException, FileNotFoundException {
    SchemaFactory schemaFactory = SchemaFactory.newInstance( XMLConstants.W3C_XML_SCHEMA_NS_URI );
    Schema  schema   = ( xsdSchema == null || xsdSchema.trim().length() == 0 )
      ? null : schemaFactory.newSchema( new File( xsdSchema ) );
    JAXBContext jaxbContext   = JAXBContext.newInstance( ObjectFactory.class.getPackage()
      .getName() );


    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    unmarshaller.setSchema( schema );

    DataSource source = (DataSource) unmarshaller.unmarshal(new
      FileInputStream(xmlDatei));

    System.out.println(source);

//CSVFile csvFile = (CSVFile) unmarshaller.unmarshal(new FileInputStream
//  (xmlDatei));

//    System.out.println(csvFile.getName());

    return null;
//    CSVFile object = CSVFile.class.cast( unmarshaller.unmarshal( new File( xmlDatei
//    ) ) );
//
//    System.out.println(object);
//
//
//    return CSVFile.class.cast( unmarshaller.unmarshal( new File( xmlDatei ) ) );
  }



  public static void parseSAX(String xsdFile) throws JAXBException,
    SAXException, ParserConfigurationException, IOException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(true);
    factory.setNamespaceAware(true);

    SAXParser parser = factory.newSAXParser();
    parser.setProperty("",
       new File(xsdFile));

    XMLReader reader = parser.getXMLReader();

    reader.setErrorHandler(new ErrorHandler(){
      public void warning(SAXParseException e) throws SAXException {
        System.out.println(e.getMessage());
      }

      public void error(SAXParseException e) throws SAXException {
        System.out.println(e.getMessage());
      }

      public void fatalError(SAXParseException e) throws SAXException {
        System.out.println(e.getMessage());
      }

    });
    reader.parse(new InputSource("document.xml"));

  }



  public static void parseXsdDom(String xsdFile) throws ParserConfigurationException,
    IOException,
    SAXException {
    //Get Document Builder
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();

    //Build Document
    Document document = builder.parse(new File(xsdFile));

    //Normalize the XML Structure; It's just too important !!
    document.getDocumentElement().normalize();

    //Here comes the root node
    Element root = document.getDocumentElement();

    //Get all employees
    NodeList nList = root.getChildNodes();
    System.out.println(root);
    System.out.println("-------");
    System.out.println(nList.getLength());
    parseXsdDomChildren(nList);
  }

  private static void parseXsdDomChildren(NodeList nodes) {
    Node node;
    for (int i = 0; i < nodes.getLength(); i++) {
      node = nodes.item(i);
      if (node.hasChildNodes()) {
        parseXsdDomChildren(node.getChildNodes());
      }
      System.out.println(node.toString());
    }
  }




  public static boolean validateXml(String xmlFile, String xsdFile){
    Source sourceFile = new StreamSource(new File(xmlFile));
    SchemaFactory schemaFactory = SchemaFactory
      .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    URL schemaFile;
    Schema schema = null;
    Validator validator;
    try {
      schemaFile = new File(xsdFile).toURI().toURL();
      schema = schemaFactory.newSchema(schemaFile);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (org.xml.sax.SAXException e) {
      e.printStackTrace();
    }
    validator = schema.newValidator();


    try {
      validator.validate(sourceFile);
      System.out.println(sourceFile.getSystemId() + " is valid");
      return true;
    } catch (org.xml.sax.SAXException|IOException e) {
      return false;
    }

  }

  public static void parseXsd(String xsdFile) throws InstantiationException,
    IllegalAccessException,
    ClassNotFoundException {
    System.setProperty(DOMImplementationRegistry.PROPERTY, "com.sun.org.apache.xerces.internal.dom.DOMXSImplementationSourceImpl");
    DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
    com.sun.org.apache.xerces.internal.impl.xs.XSImplementationImpl impl = (XSImplementationImpl) registry.getDOMImplementation("XS-Loader");
    XSLoader schemaLoader = impl.createXSLoader(null);
    XSModel model = schemaLoader.loadURI(xsdFile);


//    System.out.println(model.getComponents(XSConstants.ATTRIBUTE_GROUP));
  }



}
