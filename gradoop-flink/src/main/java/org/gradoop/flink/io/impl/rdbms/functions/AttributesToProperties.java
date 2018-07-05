package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Parses database tuples to valid EPGM properties
 */
public class AttributesToProperties {

	//parses properties with foreign key attributes included
	public static Properties getProperties(Row tuple, RowHeader rowheader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowheader.getRowHeader()) {
			props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
		}
		return props;
	}

	//parses properties without foreign key attributes
	public static Properties getPropertiesWithoutFKs(Row tuple, RowHeader rowheader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowheader.getRowHeader()) {
			if (rht.getAttType().equals(RDBMSConstants.ATTRIBUTE_FIELD)) {
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			} else if (rht.getAttType().equals(RDBMSConstants.PK_FIELD)) {
				props.set(rht.getName(), PropertyValueParser.parse((tuple.getField((rht.getPos())))));
			}
		}
		return props;
	}
}
