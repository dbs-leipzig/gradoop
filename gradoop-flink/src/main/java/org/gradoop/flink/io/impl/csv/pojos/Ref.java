//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.09.08 at 11:00:43 AM CEST 
//


package org.gradoop.flink.io.impl.csv.pojos;

import javax.xml.bind.annotation.*;
import java.io.Serializable;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;attGroup ref="{http://www.gradoop.org/csvInput}refattributes"/&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "")
@XmlRootElement(name = "ref")
public class Ref implements Serializable
{

    private final static long serialVersionUID = 1L;
    @XmlAttribute(name = "column_id")
    protected Long columnId;

    /**
     * Gets the value of the columnId property.
     * 
     * @return
     *     possible object is
     *     {@link Long }
     *     
     */
    public Long getColumnId() {
        return columnId;
    }

    /**
     * Sets the value of the columnId property.
     * 
     * @param value
     *     allowed object is
     *     {@link Long }
     *     
     */
    public void setColumnId(Long value) {
        this.columnId = value;
    }

}
