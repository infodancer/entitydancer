package org.infodancer.persist.sql;

import java.beans.IntrospectionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import javax.persistence.PersistenceException;

import org.infodancer.persist.EntityField;
import org.infodancer.persist.EntityType;
import org.infodancer.persist.dbapi.AbstractDatabaseTable;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseException;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseTable;

public class SQLDatabaseTable extends AbstractDatabaseTable implements DatabaseTable
{
	private static final Logger logger = Logger.getLogger(SQLDatabaseTable.class.getName());
	private SQLDatabase sqldb;
	
	protected SQLDatabaseTable(SQLDatabase sqldb, String name)
	{
		super(name);
		this.sqldb = sqldb;
	}

	public void addField(DatabaseField field)
	{
		field.setDatabaseTable(this);
		String fieldName = field.getName();
		if (!fields.containsKey(fieldName))
		{
			fields.put(field.getName(), (SQLDatabaseField) field);			
		}
		else
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Tried to add ");
			msg.append(fieldName);
			msg.append(" to table ");
			msg.append(name);
			msg.append(", but that field had already been defined.");
			throw new DatabaseException(msg.toString());
		}
	}

	public void delete(Object key)
	{
		DatabaseConnection con = null;
		
		try
		{
			con = sqldb.getConnection();
			delete(con, key);
		}

		finally
		{
			try { if (con != null) sqldb.putConnection(con); } catch (Exception e) {}
		}		
	}
	
	public void delete(DatabaseConnection con, Object key)
	{
		String sql = null;
		Connection sqlcon = null;
		PreparedStatement st = null;

		try
		{
			if (key != null)
			{
				sql = createPreparedDeleteStatement();
				sqlcon = (SQLConnection) con;
				st = sqlcon.prepareStatement(sql);
				sqldb.setStatementValue(st, 1, getPrimaryKey(), key);
				st.executeUpdate();
			}
			else throw new DatabaseException("Cannot find a null key!");
		}
		
		catch (Exception e)
		{
			logger.severe(sql);
			e.printStackTrace();
			throw new PersistenceException(e);
		}
		
		finally
		{
			try { if (st != null) st.close(); } catch (Exception e) {} 
		}		
	}
	
	public void merge(Object key, Map<String,Object> values)
	{
		DatabaseConnection con = null;
		
		try
		{
			con = sqldb.getConnection();
			merge(con, key, values);
		}

		finally
		{
			try { if (con != null) sqldb.putConnection(con); } catch (Exception e) {}
		}		
	}
	
	public void merge(DatabaseConnection con, Object key, Map<String,Object> values)
	{
		String sql = null;
		Connection sqlcon = null;
		PreparedStatement st = null;

		try
		{
			int fieldcount = 1;
			sqlcon = (SQLConnection) con;
			DatabaseField primaryKeyField = getPrimaryKey();
			sql = createPreparedUpdateStatement(values);
			st = sqlcon.prepareStatement(sql);
			for (String fieldname : values.keySet())
			{
				DatabaseField field = fields.get(fieldname);
				if (!field.isPrimaryKey())
				{
					Object value = values.get(fieldname);
					sqldb.setStatementValue(st, fieldcount++, field, value);
				}
			}
			sqldb.setStatementValue(st, fieldcount++, primaryKeyField, key);
			st.execute();
		}
		
		catch (SQLException e)
		{
			logger.severe(sql);
			e.printStackTrace();
			throw new DatabaseException(e);
		}

		catch (Exception e)
		{
			logger.severe(sql);
			e.printStackTrace();
			throw new PersistenceException(e);
		}		
		
		finally
		{
			try { if (st != null) st.close(); } catch (Exception e) {} 
		}		
	}
	
	public Object persist(Map<String,Object> values)
	{
		DatabaseConnection con = null;
		
		try
		{
			con = sqldb.getConnection();
			return persist(con, values);
		}
		
		finally
		{
			try { if (con != null) sqldb.putConnection(con); } catch (Exception e) {}
		}		
	}
	
	public Object persist(DatabaseConnection con, Map<String,Object> values)
	{
		logger.entering("SQLDatabaseTable", "persist(Map<String,Object> values)");
		logValueMap(values);
		String query = null;
		Connection sqlcon = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		try
		{
			int fieldcount = 1;
			query = createPreparedInsertStatement(values);
			sqlcon = (SQLConnection) con;
			st = sqlcon.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
			for (String fieldname : values.keySet())
			{
				DatabaseField field = fields.get(fieldname);
				if (field != null)
				{
					logger.finest("Setting parameter " + fieldname + "[" + fieldcount + "] to " + values.get(fieldname));
					sqldb.setStatementValue(st, fieldcount++, field, values.get(fieldname));
				}
				else 
				{
					StringBuilder msg = new StringBuilder();
					msg.append(fieldname);
					msg.append(" is not a field in this table! [candidates:");
					for (String name : fields.keySet())
					{
						msg.append(" \"");
						msg.append(name);
						msg.append("\"");
					}
					msg.append("]");
					throw new DatabaseException(msg.toString());
				}
			}
			st.executeUpdate();
			rs = st.getGeneratedKeys();
			if (rs.next())
			{
				Object result = rs.getObject(1);
				return result;
			}
			else return null; 
		}
		
		catch (SQLException e)
		{
			logger.severe(query);
			for (String fieldname : values.keySet())
			{
				logger.severe(fieldname + ":" + values.get(fieldname));
			}
			e.printStackTrace();
			throw new DatabaseException(e);
		}

		catch (Exception e)
		{
			logger.severe(query);
			for (String fieldname : values.keySet())
			{
				logger.severe(fieldname + ":" + values.get(fieldname));
			}
			e.printStackTrace();
			throw new DatabaseException(e);
		}
		
		finally
		{
			try { if (rs != null) rs.close(); } catch (Exception e) {}
			try { if (st != null) st.close(); } catch (Exception e) {}
		}		
	}

	public Map<String,Object> find(Object key)
	{
		DatabaseConnection con = null;
		
		try
		{
			con = sqldb.getConnection();
			return find(con, key);
		}
		
		finally
		{
			try { if (con != null) sqldb.putConnection(con); } catch (Exception e) {}
		}
	}
	
	public Map<String,Object> find(DatabaseConnection con, Object key)
	{
		SQLConnection sqlcon = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try
		{
			if (key != null)
			{
				Map<String,Object> result = new TreeMap<String,Object>();
				String query = createPreparedFindStatement();
				sqlcon = (SQLConnection) con;
				st = sqlcon.prepareStatement(query);
				sqldb.setStatementValue(st, 1, getPrimaryKey(), key);
				rs = st.executeQuery();
				if (rs.next())
				{
					for (String name : fields.keySet())
					{
						DatabaseField field = fields.get(name);
						if ((field != null) && (!field.isPrimaryKey()))
						{
							// Store this in a variable for ease of debugging
							Object value = sqldb.getResultSetValue(this, field, key, rs);
							result.put(name, value);
						}
					}
				}
				return result;
			}
			else throw new DatabaseException("Cannot find a null key!");
		}
		
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}		

		catch (SQLException e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}
		
		catch (IntrospectionException e)
		{
			e.printStackTrace();
			throw new DatabaseException(e);
		}		
		
		finally
		{
			try { if (rs != null) rs.close(); } catch (Exception e) {}
			try { if (st != null) st.close(); } catch (Exception e) {}
		}
	}
	
	protected String createPreparedFindStatement()
	throws ClassNotFoundException, IntrospectionException
	{
		StringBuilder result = new StringBuilder();
		result.append("SELECT ");
		boolean first = true;
		for (DatabaseField field : fields.values())
		{
			if (first) first = false;
			else result.append(',');
			result.append(field.getName());
		}		
		result.append(" FROM ");
		result.append(name);
		result.append(" WHERE ");
		result.append(getPrimaryKeyName());
		result.append(" = ?");
		return result.toString();
	}

	protected String createPreparedUpdateStatement(Map<String,Object> values)
	throws ClassNotFoundException, IntrospectionException
	{
		StringBuilder result = new StringBuilder();
		result.append("UPDATE ");
		result.append(name);
		result.append(" SET ");
		
		boolean first = true;
		for (String fieldname : values.keySet())
		{
			DatabaseField field = fields.get(fieldname);
			if (field != null)
			{
				if (!field.isPrimaryKey())
				{
					if (first) first = false;
					else result.append(", ");
					result.append(field.getName());
					result.append(" = ?");
				}
			}
			else throw new DatabaseException("Field " + fieldname + " does not exist in table " + name);
		}		
		result.append(" WHERE ");
		result.append(getPrimaryKeyName());
		result.append(" = ?");
		return result.toString();
	}

	protected String createPreparedDeleteStatement()
	throws SQLException
	{
		StringBuilder result = new StringBuilder();
		result.append("DELETE ");
		result.append(" FROM ");
		result.append(name);
		result.append(" WHERE ");
		result.append(getPrimaryKeyName());
		result.append(" = ?");
		return result.toString();
	}
	
	
	/** 
	 * Creates a prepared insert statement string.  
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IntrospectionException
	 */
	protected String createPreparedInsertStatement(Map<String,Object> values)
	throws SQLException, ClassNotFoundException, IntrospectionException
	{
		int fieldcount = 0;
		StringBuilder result = new StringBuilder();
		result.append("INSERT INTO " + name);
		result.append("(");
		boolean first = true;
		// If the key is not a generated key, include it
		for (String fieldname : values.keySet())
		{
			fieldcount++;
			if (first) first = false;
			else result.append(',');
			result.append(fieldname);
		}
		result.append(") VALUES (");
		first = true;
		for (int i = 0; i < fieldcount; i++)
		{
			if (first) first = false;
			else result.append(',');
			result.append('?');
		}
		result.append(')');
		return result.toString();
	}
	
	protected String createPreparedRelationshipClear(EntityType type, EntityField field)
	throws SQLException, ClassNotFoundException, IntrospectionException
	{		
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");
		sql.append(field.getRelationshipTableName());
		sql.append(" WHERE ");
		sql.append(type.getTableName());
		sql.append("_");
		sql.append(field.getName());
		sql.append(" = ?");
		return sql.toString();
	}

	@Override
	public void clear(Map<String, Object> values)
	{
		DatabaseConnection con = null;
		
		try
		{
			con = sqldb.getConnection();
			clear(con, values);
		}
		
		finally
		{
			try { if (con != null) sqldb.putConnection(con); } catch (Exception e) {}			
		}
	}
	
	public void clear(DatabaseConnection con, Map<String, Object> values)
	{
		String sql = null;
		Connection sqlcon = null;
		PreparedStatement st = null;

		try
		{
			int i = 1;
			sqlcon = (SQLConnection) con;
			sql = createPreparedTableClearStatement(values);
			logger.fine("SQLDatabaseTable.clear(values): " + sql);
			st = sqlcon.prepareStatement(sql);
			for (String name : values.keySet())
			{
				Object value = values.get(name);
				logger.fine("clear: " + name + " = " + value);
				sqldb.setStatementValue(st, i++, fields.get(name), value);
			}
			st.execute();
		}
		
		catch (SQLException e)
		{
			logger.severe(sql.toString());
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

		}				
	}

	@Override
	public void clear()
	{
		String sql = null;
		Connection con = null;
		PreparedStatement st = null;

		try
		{
			con = sqldb.getConnection();
			sql = createPreparedTableClearStatement();
			st = con.prepareStatement(sql);
			st.execute();
		}
		
		catch (SQLException e)
		{
			logger.severe(sql.toString());
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

	private String createPreparedTableClearStatement()
	{
		return "DELETE FROM " + getName();
	}

	private String createPreparedTableClearStatement(Map<String,Object> values)
	{
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");
		sql.append(getName());
		sql.append(" WHERE ");
		boolean first = true;
		for (String key : values.keySet())
		{
			if (key != null)
			{
				DatabaseField field = fields.get(key);
				if (field != null)
				{
					if (first) first = false;
					else sql.append(" AND ");
					sql.append(key);
					sql.append(" = ?");
				}
				else 
				{
					StringBuilder msg = new StringBuilder();
					msg.append(key);
					msg.append(" is not a recognized field in ");
					msg.append(name);
					msg.append(" [candidates:");
					for (String name : fields.keySet())
					{
						msg.append(" \"");
						msg.append(name);
						msg.append("\"");
					}
					msg.append("]");
					throw new DatabaseException(msg.toString());
				}
			}
		}
		return sql.toString();
	}

	@Override
	public void alterTable(DatabaseField field)
	{
		String sql = null;
		Connection con = null;
		PreparedStatement st = null;

		try
		{
			con = sqldb.getConnection();
			sql = createAlterTableStatement(field);
			logger.warning(sql);
			st = con.prepareStatement(sql);
			st.execute();
			fields.put(field.getName(), field);
		}
		
		catch (SQLException e)
		{
			logger.severe(sql.toString());
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

	private String createAlterTableStatement(DatabaseField field)
	{
		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE ");
		sql.append(name);
		sql.append(" ADD COLUMN (");
		sql.append(field.getName());
		sql.append(" ");
		sql.append(sqldb.createFieldDefinition(field));
		sql.append(")");
		return sql.toString();
	}

}
