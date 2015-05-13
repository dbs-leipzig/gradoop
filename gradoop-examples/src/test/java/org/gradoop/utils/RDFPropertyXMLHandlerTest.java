package org.gradoop.utils;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Parse labels for 3 example URIs.
 */
public class RDFPropertyXMLHandlerTest {
  private static final Logger LOG = Logger.getLogger(RDFPropertyXMLHandler
    .class);
  public static final String URI_FACTBOOK =
    "http://www4.wiwiss.fu-berlin.de/factbook/resource/Afghanistan";
  public static final String URI_DBPEDIA = "http://dbpedia.org/resource/Berlin";
  public static final String URI_NYTIMES =
    "http://data.nytimes.com/N50987186835223032381";
  public static final String URI_GEO = "http://sws.geonames.org/2879139/";
  public static final String URI_LGD =
    "http://linkedgeodata.org/triplify/node240111242";
  // resource exists, but has no label at all
  private static final String FAIL =
    "http://dbpedia.org/resource/Parker_Drilling_Company";


  private static final String[] LABELS = new String[]{"rdfs:label",
                                                      "skos:prefLabel",
                                                      "gn:name"};

  @Test
  public void testHandler() throws IOException, SAXException,
    ParserConfigurationException {

    RDFPropertyXMLHandler parser = new RDFPropertyXMLHandler();

    checkForLabel(parser, URI_FACTBOOK);
    checkForLabel(parser, URI_DBPEDIA);
    checkForLabel(parser, URI_NYTIMES);
    checkForLabel(parser, URI_GEO);
    checkForLabel(parser, URI_LGD);
    checkForLabel(parser, FAIL);
  }

  public void checkForLabel(RDFPropertyXMLHandler parser, String url) throws
    ParserConfigurationException, SAXException, IOException {
    LOG.info("checkForLabel: " + url);

    try {
      HashSet<String[]> labelProperties = parser.getLabelsForURI(url);

      if (labelProperties.isEmpty()) {
        if (!url.equals(FAIL)) {
          LOG.error("URL " + url + " not reachable, try again later.");
        } else {
          assertEquals(url, FAIL);
        }
      }

      for (String[] propertyAndValue : labelProperties) {
        boolean isProperty = false;
        for (String label : LABELS) {
          if (propertyAndValue[0].contains(label)) {
            assertTrue(true);
            isProperty = true;
            break;
          }
        }
        if (!isProperty) {
          assertTrue(false);
        }
      }
    } catch (SAXException | ConnectTimeoutException | SocketTimeoutException
      e) {
      LOG.error("Error on URL " + url + " , try again later:"
        + e.toString());
    }
  }
}