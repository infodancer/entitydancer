package org.infodancer.persist.sql;

import java.beans.IntrospectionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.PersistenceException;
import javax.sql.DataSource;

import org.infodancer.persist.dbapi.AbstractDatabase;
import org.infodancer.persist.dbapi.Database;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseConnectionPool;
import org.infodancer.persist.dbapi.DatabaseException;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseTable;

public class SQLDatabase extends AbstractDatabase implements Database
{
	private static final Logger logger = Logger.getLogger(SQLDatabase.class.getClass().getName());
	protected SQLConnectionFactory factory;
	protected DatabaseConnectionPool<SQLConnection> pool;
	protected Map<String,DatabaseTable> tables = new TreeMap<String,DatabaseTable>();

	@Override
	public void initialize(Properties properties)
	{
		try
		{
			this.properties = properties;
			initialize();
		}
		
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public SQLConnection getConnection()
	{
		return pool.getConnection();
	}
	
	public void putConnection(SQLConnection con)
	{
		pool.putConnection(con);
	}
	
	@Override
	public DatabaseTable getTable(String name)
	{
		return tables.get(name);
	}

	@Override
	public Collection<DatabaseTable> getTables()
	{
		return Collections.unmodifiableCollection(tables.values());
	}

	@Override
	public void dropTable(DatabaseTable table)
	{
		dropTable(table.getName());
	}

	@Override
	public void createTable(DatabaseTable table)
	{
		validateTable(table);
		StringBuilder sql = null;
		Connection con = null;
		Statement st = null;
		
		try
		{
			sql = new StringBuilder();
			sql.append("CREATE TABLE ");
			sql.append(table.getName());
			sql.append('(');
			// Define the primary key first, just to keep the resulting table pretty
			DatabaseField pkey = table.getPrimaryKey();
			sql.append(pkey.getName());
			sql.append(' ');
			String keydef = pkey.getDefinition();
			if (keydef != null) sql.append(keydef);
			else sql.append(createFieldDefinition(pkey));			
			// Define the rest of the fields
			for (DatabaseField field : table.getFields())
			{
				if (!field.isPrimaryKey())
				{
					sql.append(", ");
					sql.append(field.getName());
					sql.append(' ');
					String definition = field.getDefinition();
					if (definition != null) sql.append(definition);
					else sql.append(createFieldDefinition(field));
					if (field.isIndexed())
					{
						sql.append(", ");
						sql.append("INDEX ");
						sql.append(field.getName());
						sql.append("_idx");
						sql.append(" (");
						sql.append(field.getName());
						sql.append(")");
					}
				}
			}
			sql.append(')');
			con = pool.getConnection();
			st = con.createStatement();
			st.execute(sql.toString());
			tables.put(table.getName(), table);
		}
		
		catch (SQLException e)
		{
			System.err.println(sql.toString());
			e.printStackTrace();
			StringBuilder msg = new StringBuilder();
			msg.append("Error processing query: " + sql.toString());
			logger.severe(msg.toString());
			throw new DatabaseException(e);
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}
		
		finally
		{
			try { if (st != null) st.close(); } catch (Exception e) {}
			try { if (con != null) con.close(); } catch (Exception e) {}			
		}
	}

	String createFieldDefinition(DatabaseField field)
	{
		StringBuilder sql = new StringBuilder();
		switch (field.getSqlType())
		{
		  	case java.sql.Types.BOOLEAN:
			{		
				sql.append("BOOLEAN DEFAULT 0");
				break;
			}
			
		  	case java.sql.Types.CHAR:
			{		
				sql.append("CHAR");
				int length = field.getLength();
				if (length > 0) sql.append("(" + length + ")");
				break;
			}
		  		
			case java.sql.Types.VARCHAR: 
			{		
				sql.append("VARCHAR");
				int length = field.getLength();
				if (length > 0) sql.append("(" + length + ")");
				else sql.append("(128)");
				break;
			}

			case java.sql.Types.BINARY:
			{		
				sql.append("BINARY");
				int length = field.getLength();
				if (length > 0) sql.append("(" + length + ")");
				else sql.append("(128)");
				break;
			}
				
			case java.sql.Types.VARBINARY:
			{		
				sql.append("VARBINARY");
				int length = field.getLength();
				if (length > 0) sql.append("(" + length + ")");
				else sql.append("(128)");
				break;
			}

			case java.sql.Types.BIGINT:
			{		
				sql.append("BIGINT");
				break;
			}

			case java.sql.Types.FLOAT:
			{		
				sql.append("FLOAT");
				break;
			}

			case java.sql.Types.DOUBLE:
			{		
				sql.append("DOUBLE");
				break;
			}
			
			case java.sql.Types.INTEGER:
			{		
				sql.append("INTEGER");
				break;
			}

			case java.sql.Types.NUMERIC:
			{		
				sql.append("NUMERIC");
				break;
			}

			case java.sql.Types.DATE:
			{		
				sql.append("DATE");
				break;
			}

			case java.sql.Types.TIME:
			{		
				sql.append("TIME");
				break;
			}

			case java.sql.Types.TIMESTAMP:
			{		
				if (isMySQL()) sql.append("DATETIME");
				else sql.append("TIMESTAMP");
				break;
			}

			case java.sql.Types.BLOB:
			{		
				sql.append("BLOB");
				break;
			}

			case java.sql.Types.CLOB:
			{		
				sql.append("CLOB");
				break;
			}
		}
		
		if (field.isPrimaryKey())
		{
			sql.append(" PRIMARY KEY");
			if (field.isGeneratedKey())
			{
				sql.append(" AUTO_INCREMENT");
			}
		}
		
		if (field.isUnique())
		{
			sql.append(" UNIQUE ");
		}
		
		if (!field.isNullable())
		{
			sql.append(" NOT NULL ");
		}

		return sql.toString();
	}


	/**
	 * For now, my environment is MySQL, so that's what I support.
	 * This will be transitioned away from eventually.
	 * @return
	 */
	public boolean isMySQL()
	{
		return true;
	}

	@Override
	public void dropTable(String tableName)
	{
		StringBuilder sql = new StringBuilder();
		Connection con = null;
		Statement   st = null;
		
		try
		{
			DatabaseTable table = tables.get(tableName);
			if (table != null)
			{
				con = pool.getConnection();
				 st = con.createStatement();
				 sql.append("DROP TABLE ");
				 sql.append(tableName);
				 st.execute(sql.toString());
				 tables.remove(tableName);
			}
			else throw new DatabaseException("Cannot remove " + tableName + "; it doesn't exist!");
		}

		catch (SQLException e)
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Error processing query: " + sql);
			logger.severe(msg.toString());
			e.printStackTrace();
			throw new DatabaseException(e);
		}

		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException(e);
		}
		
		finally
		{
			try { if (st != null) st.close(); } catch (Exception e) { }
			try { if (con != null) con.close(); } catch (Exception e) { }
		}
	}

	/** 
	 * Sets the name of the datasource, which will be retrieved from the context.
	 * @param name
	 * @throws SQLException 
	 */
	public void setDataSource(String name) throws NamingException, SQLException
	{
		Context icontext = new InitialContext();
		Context context = (Context) icontext.lookup("java:/comp/env");
		DataSource datasource = (javax.sql.DataSource) context.lookup(name);
		setDataSource(datasource);
	}
	
	/**
	 * Sets the DataSource directly.
	 * @param datasource
	 * @throws SQLException 
	 */
	public void setDataSource(javax.sql.DataSource datasource) throws SQLException
	{
		this.factory = new SQLConnectionFactory(datasource);
		this.pool = new DatabaseConnectionPool<SQLConnection>(factory, new Properties());
		initialize();
	}

	protected void initializeTableList()
	{
		SQLConnection con = null;
		Statement st1 = null;
		ResultSet rs1 = null;
		
		try 
		{
			con = pool.getConnection();
			st1 = con.createStatement();
			rs1 = st1.executeQuery("SHOW TABLES");
			while (rs1.next())
			{
				String tableName = rs1.getString(1);
				DatabaseTable table = describeTable(tableName);
				tables.put(tableName, table);
			}
		}

		catch (SQLException e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}
		
		finally 
		{
			try { if (rs1 != null) rs1.close(); } catch (Exception e) { }
			try { if (st1 != null) st1.close(); } catch (Exception e) { }
			try { if (con != null) con.close(); } catch (Exception e) { } 
		}
	}

	protected void initialize()
	{
		try
		{
			if (factory == null)
			{
				this.factory = new SQLConnectionFactory(properties);
			}
			if (pool == null)
			{
				this.pool = new DatabaseConnectionPool<SQLConnection>(factory, properties);
			}
			this.pool.open();
			initializeTableList();
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}
	}

	@Override
	public DatabaseTable createTable(String tableName)
	{
		return new SQLDatabaseTable(this, tableName);
	}
	
	public void setStatementValue(PreparedStatement st, int i, DatabaseField field, Object value)
	throws SQLException, IntrospectionException, ClassNotFoundException
	{
		try
		{
			int sqltype = field.getSqlType();
			if (value != null)
			{
				switch (sqltype)
				{
					case java.sql.Types.LONGVARCHAR:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.CHAR:
					case java.sql.Types.NCHAR:
					case java.sql.Types.NVARCHAR:
					{
						st.setString(i, (String) value);
						break;
					}

					case java.sql.Types.BINARY:
					case java.sql.Types.VARBINARY:
					{
						st.setBytes(i, (byte[]) value);
						break;
					}

					case java.sql.Types.SMALLINT:
					case java.sql.Types.TINYINT:
					case java.sql.Types.INTEGER:
					{
						if (value instanceof Boolean)
						{
							st.setBoolean(i, (Boolean) value);
						}
						else if (value instanceof Enum)
						{
							st.setInt(i, ((Enum) value).ordinal());
						}
						else st.setInt(i, (Integer) value);
						break;
					}
					
					case java.sql.Types.BIGINT:
					{
						st.setLong(i, (Long) value);
						break;
					}

					case java.sql.Types.FLOAT:
					{
						st.setFloat(i, (Float) value);
						break;
					}

					case java.sql.Types.DOUBLE:
					{
						st.setDouble(i, (Double) value);
						break;
					}

					case java.sql.Types.DATE:
					{
						st.setDate(i, (java.sql.Date) value);
						break;
					}

					case java.sql.Types.TIME:
					{
						st.setTime(i, (java.sql.Time) value);
						break;
					}

					case java.sql.Types.TIMESTAMP:
					{
						java.sql.Timestamp timestamp = null;
						if (value instanceof java.sql.Timestamp)
						{
							timestamp = (Timestamp) value;
						}
						else if (value instanceof java.util.Date)
						{
							timestamp = new java.sql.Timestamp(((java.util.Date) value).getTime());
						}
						else
						{
							StringBuilder msg = new StringBuilder();
							msg.append("Field ");
							msg.append(field.getName());
							msg.append(" expects java.util.Timestamp;");
							msg.append(value.getClass().getName());
							msg.append(" is not convertable!");
							throw new DatabaseException(msg.toString());	
						}
						st.setTimestamp(i, timestamp);
						break;
					}

					case java.sql.Types.BOOLEAN:
					{
						st.setBoolean(i, (Boolean) value);
						break;
					}

					case java.sql.Types.OTHER:
					{
						st.setObject(i, value);
						break;
					}
					
					default: 
					{
						StringBuilder msg = new StringBuilder();
						msg.append("Could not persist field ");
						msg.append(field.getName());
						msg.append(" because the SQLType ");
						msg.append(Integer.toString(sqltype));
						msg.append(" is not supported!");
						logger.severe(msg.toString());
						throw new PersistenceException(msg.toString()); 
					}
				}
			}
			else st.setNull(i, sqltype);
		}
		
		catch (SQLException e)
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Could not persist field ");
			msg.append(field.getName());
			logger.severe(msg.toString());
			throw new PersistenceException(e); 
		}
	}

	public Object getResultSetValue(SQLDatabaseTable table, DatabaseField field, Object key, ResultSet rs) 
	throws SQLException 
	{
		String fieldname = field.getName();
		try
		{
			switch (field.getSqlType())
			{
				case java.sql.Types.LONGVARCHAR:
				case java.sql.Types.VARCHAR:
				case java.sql.Types.CHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.NVARCHAR:
				{
					return rs.getString(fieldname);
				}

				case java.sql.Types.INTEGER:
				{
					return rs.getInt(fieldname);
				}
				
				case java.sql.Types.BIGINT:
				{
					return rs.getLong(fieldname);
				}

				case java.sql.Types.FLOAT:
				{
					return rs.getFloat(fieldname);
				}

				case java.sql.Types.DOUBLE:
				{
					return rs.getDouble(fieldname);
				}

				case java.sql.Types.DATE:
				{
					return rs.getDate(fieldname);
				}

				case java.sql.Types.TIMESTAMP:
				{
					return rs.getTimestamp(fieldname);
				}

				case java.sql.Types.BLOB:
				{
					if (isMySQL())
					{
						return new SQLBlob(this, table, field, key);
					}
				}
				
				default: 
				{
					return rs.getObject(fieldname);
				}
			}
		}
		
		catch (SQLException e)
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Could not retrieve field ");
			msg.append(fieldname);
			logger.severe(msg.toString());
			throw new PersistenceException(e); 
		}
	}

	@Override
	public boolean isOpen()
	{
		if (pool != null) 
		{
			if (pool.size() > 0) return true;
			else return false;
		}
		else return false;
	}

	@Override
	public void close()
	{
		if (pool != null) pool.close();
	}
	
	@Override
	public void updateTable(DatabaseTable table)
	{
		dropTable(table);
		createTable(table);
	}

	@Override
	public DatabaseField createField(String fieldName, int sqlType)
	{
		return new SQLDatabaseField(fieldName, sqlType);
	}

	@Override
	public DatabaseTable describeTable(String tableName)
	{
		SQLConnection con = null;
		Statement st1 = null;
		ResultSet rs1 = null;
		
		try 
		{
			logger.finest("Describing table: " + tableName);
			con = pool.getConnection();
			st1 = con.createStatement();
			rs1 = st1.executeQuery("DESCRIBE " + tableName);
			DatabaseTable result = createTable(tableName);
			while (rs1.next())
			{
				String fieldName  = rs1.getString(1);
				String fieldType  = rs1.getString(2);
				String fieldNull  = rs1.getString(3);
				String fieldKey   = rs1.getString(4);
				String fieldDef   = rs1.getString(5);
				String fieldExtra = rs1.getString(6);
				
				logger.finest("FieldName: " + fieldName);
				logger.finest("FieldType: " + fieldType);
				logger.finest("FieldNull: " + fieldNull);
				logger.finest("FieldKey: " + fieldKey);
				logger.finest("FieldDef: " + fieldDef);
				logger.finest("FieldExtra: " + fieldExtra);
				
				DatabaseField field = createField(fieldName, getSQLType(fieldType));
				field.setLength(parseLength(fieldType));
				field.setNullable(checkNullable(fieldNull));
				field.setPrimaryKey(checkPrimaryKey(fieldKey));
				field.setGeneratedKey(parseGeneratedKey(fieldExtra));
				result.addField(field);
			}
			return result;
		}

		catch (SQLException e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}
		
		finally 
		{
			try { if (rs1 != null) rs1.close(); } catch (Exception e) { }
			try { if (st1 != null) st1.close(); } catch (Exception e) { }
			try { if (con != null) con.close(); } catch (Exception e) { } 
		}
	}

	private int parseLength(String fieldType)
	{
		if (fieldType != null)
		{
			int begin = fieldType.indexOf('(');
			int end = fieldType.indexOf(')');
			if ((begin != -1) && (end != -1))
			{
				String length = fieldType.substring(begin + 1, end);
				return Integer.parseInt(length);
			}
		}
		return 0;
	}

	private boolean parseGeneratedKey(String fieldExtra)
	{
		return "auto increment".equalsIgnoreCase(fieldExtra);
	}

	private boolean checkPrimaryKey(String fieldKey)
	{
		return "PRI".equalsIgnoreCase(fieldKey);
	}

	private boolean checkNullable(String fieldNull)
	{
		return "YES".equalsIgnoreCase(fieldNull);
	}

	private int getSQLType(String fieldType)
	{
		if (fieldType != null)
		{
			String type = fieldType;
			int begin = fieldType.indexOf('(');
			int end = fieldType.indexOf(')');
			if ((begin != -1) && (end != -1))
			{
				type = fieldType.substring(0, begin);
			}
			
			if ("int".equalsIgnoreCase(type)) return java.sql.Types.INTEGER;
			if ("enum".equalsIgnoreCase(type)) return java.sql.Types.INTEGER;
			else if ("varchar".equalsIgnoreCase(type)) return java.sql.Types.VARCHAR;
			else if ("char".equalsIgnoreCase(type)) return java.sql.Types.CHAR;
			else if ("text".equalsIgnoreCase(type)) return java.sql.Types.CHAR;
			else if ("bigint".equalsIgnoreCase(type)) return java.sql.Types.BIGINT;
			else if ("smallint".equalsIgnoreCase(type)) return java.sql.Types.SMALLINT;
			else if ("tinyint".equalsIgnoreCase(type)) return java.sql.Types.TINYINT;
			else if ("float".equalsIgnoreCase(type)) return java.sql.Types.FLOAT;
			else if ("double".equalsIgnoreCase(type)) return java.sql.Types.DOUBLE;
			else if ("decimal".equalsIgnoreCase(type)) return java.sql.Types.DECIMAL;
			else if ("boolean".equalsIgnoreCase(type)) return java.sql.Types.BOOLEAN;
			else if ("date".equalsIgnoreCase(type)) return java.sql.Types.DATE;
			else if ("timestamp".equalsIgnoreCase(type)) return java.sql.Types.TIMESTAMP;
			else return java.sql.Types.OTHER;
		}
		else return 0;
	}

	@Override
	public DatabaseQuery createQuery()
	{
		return new SQLDatabaseQuery(this);
	}

	@Override
	public void alterTable(DatabaseTable newtable)
	{
		throw new DatabaseException("Operation not yet implemented!");
	}

	@Override
	public void putConnection(DatabaseConnection con)
	{
		if (!con.isAutoCommit()) 
		{
			con.rollback();
			con.setAutoCommit(true);
		}
		pool.putConnection((SQLConnection) con);
	}
}
