package org.infodancer.persist.sql;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.infodancer.persist.dbapi.DatabaseField;

public class SQLBlob implements java.sql.Blob
{
	SQLConnection con;
	SQLDatabase db;
	SQLDatabaseTable table;
	DatabaseField field;
	Object key;
	Blob blob;
	
	public SQLBlob(SQLDatabase sqlDatabase, SQLDatabaseTable table, DatabaseField field, Object key)
	{
		this.db = sqlDatabase;
		this.table = table;
		this.field = field;
		this.key = key;
	}

	public SQLBlob(SQLDatabase sqlDatabase, SQLConnection con, SQLDatabaseTable table, DatabaseField field, Object key)
	{
		this.con = con;
		this.db = sqlDatabase;
		this.table = table;
		this.field = field;
		this.key = key;
	}
	
	private String createBlobQuery()
	{
		StringBuilder q = new StringBuilder();
		q.append("SELECT ");
		q.append(table.getPrimaryKeyName());
		q.append(",");
		q.append(field.getName());
		q.append(" FROM ");
		q.append(table.getName());
		q.append(" WHERE ");
		q.append(table.getPrimaryKeyName());
		q.append(" = ?");	
		return q.toString();
	}
	
	private String createBlobUpdate()
	{
		StringBuilder q = new StringBuilder();
		q.append("UPDATE ");
		q.append(table.getName());
		q.append(" SET ");
		q.append(field.getName());
		q.append(" = ? ");
		q.append(" WHERE ");
		q.append(table.getPrimaryKeyName());
		q.append(" = ?");	
		return q.toString();
	}

	/**
	 * Either retrieve the Blob from the field, or create one.
	 * We can't close resources here, since the user will be calling various methods on the returned object.
	 * We must rely on the user calling free().
	 * @return
	 * @throws SQLException
	 */
	private Blob getBlob() throws SQLException
	{
		if (con == null) con = db.getConnection();
		if (blob == null)
		{
			PreparedStatement st = con.prepareStatement(createBlobQuery(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			st.setObject(1, key);
			ResultSet rs = st.executeQuery();
			if (rs.next())
			{
				blob = rs.getBlob(field.getName());
				if (blob == null) 
				{
					blob = con.createBlob();
				}
			}
		}
		return blob;
	}

	/**
	 * Writes the blob back to the database.
	 * @throws SQLException
	 */
	private void setBlob() throws SQLException
	{
		if (con == null) con = db.getConnection();
		if (blob != null)
		{
			PreparedStatement st = con.prepareStatement(createBlobUpdate(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			st.setBlob(1, blob);
			st.setObject(2, key);
			st.executeUpdate();
		}
	}
	
	@Override
	public void free() throws SQLException
	{
		setBlob();
		if (blob != null) blob.free();
		if (con != null) con.close();
		blob = null;
		con = null;
	}

	@Override
	public InputStream getBinaryStream() throws SQLException
	{
		return getBlob().getBinaryStream();
	}

	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException
	{
		return getBlob().getBinaryStream(pos, length);
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException
	{
		return getBlob().getBytes(pos, length);
	}

	@Override
	public long length() throws SQLException
	{
		return getBlob().length();
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException
	{
		return getBlob().position(pattern, start);
	}

	@Override
	public long position(Blob pattern, long start) throws SQLException
	{
		return getBlob().position(pattern, start);
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException
	{
		if (con == null) con = db.getConnection();
		if (blob == null) blob = con.createBlob();
		return blob.setBinaryStream(pos);
	}

	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException
	{
		return getBlob().setBytes(pos, bytes);
	}

	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException
	{
		return getBlob().setBytes(pos, bytes, offset, len);
	}

	@Override
	public void truncate(long len) throws SQLException
	{
		getBlob().truncate(len);
	}

}
