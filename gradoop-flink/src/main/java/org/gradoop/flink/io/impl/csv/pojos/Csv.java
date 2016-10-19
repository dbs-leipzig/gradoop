//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.09.28 at 03:19:33 PM CEST 
//


package org.gradoop.flink.io.impl.csv.pojos;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojo}columns"/&gt;
 *         &lt;choice&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojo}graphhead"/&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojo}vertex"/&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojo}edge"/&gt;
 *         &lt;/choice&gt;
 *       &lt;/sequence&gt;
 *       &lt;attGroup ref="{http://www.gradoop.org/flink/io/impl/csv/pojo}csvattributes"/&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "columns",
    "graphhead",
    "vertex",
    "edge"
})
@XmlRootElement(name = "csv")
public class Csv {

    @XmlElement(required = true)
    protected Columns columns;
    protected Graphhead graphhead;
    protected Vertex vertex;
    protected Edge edge;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "charset")
    protected String charset;
    @XmlAttribute(name = "separator")
    protected String separator;

    @XmlTransient
    private String datasourceName;
    @XmlTransient
    private String domainName;
    /**
     * Gets the value of the columns property.
     * 
     * @return
     *     possible object is
     *     {@link Columns }
     *     
     */
    public Columns getColumns() {
        return columns;
    }

    /**
     * Sets the value of the columns property.
     * 
     * @param value
     *     allowed object is
     *     {@link Columns }
     *     
     */
    public void setColumns(Columns value) {
        this.columns = value;
    }

    /**
     * Gets the value of the graphhead property.
     * 
     * @return
     *     possible object is
     *     {@link Graphhead }
     *     
     */
    public Graphhead getGraphhead() {
        return graphhead;
    }

    /**
     * Sets the value of the graphhead property.
     * 
     * @param value
     *     allowed object is
     *     {@link Graphhead }
     *     
     */
    public void setGraphhead(Graphhead value) {
        this.graphhead = value;
    }

    /**
     * Gets the value of the vertex property.
     * 
     * @return
     *     possible object is
     *     {@link Vertex }
     *     
     */
    public Vertex getVertex() {
        return vertex;
    }

    /**
     * Sets the value of the vertex property.
     * 
     * @param value
     *     allowed object is
     *     {@link Vertex }
     *     
     */
    public void setVertex(Vertex value) {
        this.vertex = value;
    }

    /**
     * Gets the value of the edge property.
     * 
     * @return
     *     possible object is
     *     {@link Edge }
     *     
     */
    public Edge getEdge() {
        return edge;
    }

    /**
     * Sets the value of the edge property.
     * 
     * @param value
     *     allowed object is
     *     {@link Edge }
     *     
     */
    public void setEdge(Edge value) {
        this.edge = value;
    }

    /**
     * Gets the value of the name property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setName(String value) {
        this.name = value;
    }

    /**
     * Gets the value of the charset property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCharset() {
        return charset;
    }

    /**
     * Sets the value of the charset property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCharset(String value) {
        this.charset = value;
    }

    /**
     * Gets the value of the separator property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * Sets the value of the separator property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSeparator(String value) {
        this.separator = value;
    }

    public String getDatasourceName() {
        return datasourceName;
    }

    public void setDatasourceName(String datasourceName) {
        this.datasourceName = datasourceName;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }
}
