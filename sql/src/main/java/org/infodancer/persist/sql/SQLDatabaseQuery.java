package org.infodancer.persist.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.infodancer.persist.dbapi.AbstractDatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseException;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseTable;
import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;

public class SQLDatabaseQuery extends AbstractDatabaseQuery implements DatabaseQuery
{
	private static final Logger logger = Logger.getLogger(SQLDatabaseQuery.class.getName());
	SQLDatabase sqldb;
	Connection con = null;
	Statement   st = null;
	ResultSet   rs = null;
	
	public SQLDatabaseQuery(SQLDatabase database)
	{
		super(database);
		this.sqldb = database;
	}
	
	public long size()
	{
		String query = null;
		Statement st1 = null;
		ResultSet rs1 = null;
		Connection con1 = null;
		
		try
		{
			query = buildSizeQuery();
			con1 = sqldb.getConnection();
			st1  = con1.createStatement();
			rs1 = st1.executeQuery(query);
			if (rs1.next()) 
			{
				long result = rs1.getLong(1);
				if ((limit > 0) && (result > limit)) result = limit;
				return result;
			}
			else return 0;
		}

		catch (UnknownTableException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}		

		catch (SQLException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}	
		
		finally
		{
			try { if (rs1 != null) rs1.close(); } catch (Exception e) { } 
			try { if (st1 != null) st1.close(); } catch (Exception e) { } 
			try { if (con1 != null) con1.close(); } catch (Exception e) { } 
		}
	}

	public void executeQuery()
	{
		con = sqldb.getConnection();
		executeQuery(con);
	}

	public void executeQuery(Connection con)
	{
		String query = null;
		
		try
		{
			query = buildQuery();
			logger.fine(query);
			st  = con.createStatement();
			rs  = st.executeQuery(query);
			rs.next();
		}
		
		catch (UnknownTableException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}		
		
		catch (SQLException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}		
	}

	@Override
	public int executeUpdate()
	{
		con = sqldb.getConnection();
		return executeUpdate(con);
	}


	public int executeUpdate(Connection con)
	{
		String query = null;
		
		try
		{
			query = buildQuery();
			logger.fine(query);
			st  = con.createStatement();
			return st.executeUpdate(query);
		}

		catch (UnknownTableException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}		
		
		catch (SQLException e)
		{
			if (query != null) logger.severe(query);
			e.printStackTrace();
			throw new DatabaseException(e);
		}		
	}

	public String buildQuery() throws UnknownTableException
	{
		StringBuilder q = new StringBuilder();
		q.append(buildSelectClause());
		q.append(buildFromClause());
		q.append(buildWhereClause());
		q.append(buildOrderByClause());
		if (limit > 0) 
		{
			q.append(" LIMIT ");
			if (first > 0) 
			{
				q.append(Long.toString(first));
				q.append(",");
			}
			q.append(Long.toString(limit));
		}
		return q.toString();
	}

	public String buildSizeQuery() throws UnknownTableException
	{
		StringBuilder q = new StringBuilder();
		q.append("SELECT COUNT(*) ");
		q.append(buildFromClause());
		q.append(buildWhereClause());
		return q.toString();
	}

	public String buildOrderByClause()
	{
		if ((orderby != null) && (!orderby.isEmpty()))
		{
			boolean first = true;
			StringBuilder q = new StringBuilder();
			q.append(" ORDER BY ");
			for (String name : orderby)
			{
				if (first) first = false;
				else q.append(", ");
				q.append(name);
			}
			if (descending) q.append(" DESC ");
			return q.toString();
		}
		else return "";
	}
	
	public String buildSelectClause()
	{
		boolean first = true;
		StringBuilder q = new StringBuilder();
		q.append("SELECT ");
		if (fields.isEmpty())
		{
			q.append('*');
		}
		else
		{
			for (String fieldName : fields.keySet())
			{
				DatabaseField field = fields.get(fieldName);
				DatabaseTable table = field.getDatabaseTable();
				if (first) first = false;
				else q.append(',');
				q.append(table.getName());
				q.append('.');
				q.append(field.getName());
				if (!fieldName.equals(field.getName()))
				{
					q.append(" AS ");
					q.append(fieldName);
				}
			}
		}
		return q.toString();		
	}
	
	/**
	 * 
	 * @return
	 */
	public String buildFromClause() throws UnknownTableException
	{
		boolean first = true;
		StringBuilder q = new StringBuilder();
		q.append(" FROM ");
		for (String alias : tables.keySet())
		{
			if (first) first = false;
			else q.append(',');
			DatabaseTable table = tables.get(alias);
			if (table != null)
			{
				q.append(table.getName());
				if (!table.getName().equalsIgnoreCase(alias))
				{
					q.append(" AS ");
					q.append(alias);
				}
			}
			else throw new UnknownTableException(alias);
		}
		return q.toString();
	}

	public String buildWhereClause()
	{
		if (!params.isEmpty())
		{
			boolean first = true;
			StringBuilder q = new StringBuilder();
			q.append(" WHERE ");
			for (QueryParameter p : params)
			{
				if (first) first = false;
				else q.append(" AND ");
				q.append(p.toString());
			}
			return q.toString();
		}
		else return "";
	}

	@Override
	public boolean next()
	{
		try
		{
			if (rs != null) return rs.next();
			else throw new DatabaseException("No active query!");	
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);	
		}
	}

	/**
	 * Clears the query parameters, allowing this query object to be reused.
	 */
	public void clear()
	{
		
	}
	
	@Override
	public String getString(String name)
	{
		try
		{
			return rs.getString(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public Object getObject(String name)
	{
		try
		{
			return rs.getObject(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public java.util.Date getDate(String name)
	{
		try
		{
			java.sql.Timestamp ts = rs.getTimestamp(name);
			return new java.util.Date(ts.getTime());
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public java.util.Date getTimestamp(String name)
	{
		try
		{
			java.sql.Timestamp ts = rs.getTimestamp(name);
			return new java.util.Date(ts.getTime());
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}
	
	public void close()
	{
		try { if (rs != null) rs.close(); } catch (Exception e) { }
		try { if (st != null) st.close(); } catch (Exception e) { } 
		try { if (con != null) con.close(); } catch (Exception e) { } 
	}

	@Override
	public Integer getInteger(String name)
	{
		try
		{
			return rs.getInt(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public Long getLong(String name)
	{
		try
		{
			return rs.getLong(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public Float getFloat(String name)
	{
		try
		{
			return rs.getFloat(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public Double getDouble(String name)
	{
		try
		{
			return rs.getDouble(name);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	@Override
	public String getNativeQuery()
	{
		return null;
	}

	@Override
	public void addParam(String left, QueryParameterType type, String right)
	{
		addParam(new SQLQueryParameter(left, type, right));
	}

	@Override
	public void addParam(String left, String type, String right)
	{
		addParam(new SQLQueryParameter(left, type, right));
	}
}
