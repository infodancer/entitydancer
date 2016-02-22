package org.infodancer.persist.sql;

import org.infodancer.persist.dbapi.AbstractDatabaseField;
import org.infodancer.persist.dbapi.DatabaseField;

public class SQLDatabaseField extends AbstractDatabaseField implements DatabaseField
{
	protected SQLDatabaseField(String name, int sqlType)
	{
		super(name, sqlType);
	}
}
