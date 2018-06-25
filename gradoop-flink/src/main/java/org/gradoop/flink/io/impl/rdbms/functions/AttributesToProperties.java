package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.JDBCType;
import java.util.ArrayList;

import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * collects the attributes of a given database tuple and stores it as properties
 * 
 * @author pc
 *
 */
public class AttributesToProperties {

	/*
	 * properties with foreign key values included
	 */
	public static Properties getProperties(Row tuple, RowHeader rowHeader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowHeader.getRowHeader()) {
			props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
		}
		return props;
	}

	/*
	 * properties without foreign key values
	 */
	public static Properties getPropertiesWithoutFKs(Row tuple, RowHeader rowHeader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowHeader.getRowHeader()) {
			if (rht.getAttType().equals(RDBMSConstants.ATTRIBUTE_FIELD)) {
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			} else if (rht.getAttType().equals(RDBMSConstants.PK_FIELD)) {
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			}
		}
		return props;
	}
}
