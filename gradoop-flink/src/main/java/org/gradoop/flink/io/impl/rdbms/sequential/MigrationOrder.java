package org.gradoop.flink.io.impl.rdbms.sequential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class MigrationOrder {
	private ArrayList<RDBMSTable> toNodes, tables;
	private HashSet<String> toEdgesTableNames, toNodesTableNames;

	public MigrationOrder(ArrayList<RDBMSTable> tables, ArrayList<RDBMSTable> toEdges) {
		this.tables = tables;

		this.toNodes = new ArrayList<RDBMSTable>();
		this.toNodesTableNames = new HashSet<String>();

		this.toEdgesTableNames = new HashSet<String>();
		for (RDBMSTable te : toEdges) {
			this.toEdgesTableNames.add(te.getTableName());
		}
	}

	public ArrayList<RDBMSTable> tablesToNodes() {
		ArrayList<RDBMSTable> candidates = null;
		do {
			candidates = new ArrayList<RDBMSTable>();
			for (RDBMSTable table : tables) {
				String tableName = table.getTableName();
				if (!toEdgesTableNames.contains(tableName) && !toNodesTableNames.contains(tableName)) {
					if (table.getForeignKeys() == null) {
						candidates.add(table);
					} else {
						boolean containsAll = true;
						Iterator it = table.getForeignKeys().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry pair = (Entry) it.next();
							if (!toNodesTableNames.contains(pair.getValue())) {
								containsAll = false;
							}
						}
						if (containsAll) {
							candidates.add(table);
						}
					}
				}
			}
			for (RDBMSTable c : candidates) {
				toNodes.add(c);
				toNodesTableNames.add(c.getTableName());
			}
		} while (!candidates.isEmpty());
		return toNodes;
	}
}
