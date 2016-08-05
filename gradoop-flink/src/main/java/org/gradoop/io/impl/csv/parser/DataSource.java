package org.gradoop.io.impl.csv.parser;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * Created by stephan on 05.08.16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://www.gradoop.org/csvInput", name="datasource")
@XmlType(factoryClass=org.gradoop.io.impl.csv.parser.ObjectFactory.class,
  factoryMethod="createDataSource")
public class DataSource {

  @XmlElement (name = "csvfile")
//  @XmlElementWrapper(name="csvfile")
  List<CSVFile> csvFiles = null;


  public List<CSVFile> getCsvFiles() {
    return csvFiles;
  }

  public void setCsvFiles(List<CSVFile> csvFiles) {
    this.csvFiles = csvFiles;
  }

  @Override
  public String toString() {
    String string = "datasource: " + csvFiles.size() + "\n";
    for (CSVFile file : csvFiles) {
      string += file.toString() + "\n";
    }

    return string;
  }
}
