package org.gradoop.flink.io.impl.csv.pojo;

import org.gradoop.flink.io.impl.csv.parser.ObjectFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * Created by stephan on 05.08.16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://www.gradoop.org/csvInput", name="datasource")
@XmlType(factoryClass=ObjectFactory.class,
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
