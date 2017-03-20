package org.infodancer.persist.dbapi;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DatabaseConnectionPool<T extends DatabaseConnection>
{
	private static final Logger logger = Logger.getLogger(DatabaseConnectionPool.class.getName());
	protected boolean closed = false;
	protected DatabaseConnectionFactory<T> factory;
	protected LinkedBlockingDeque<T> pool = new LinkedBlockingDeque<T>();
	protected List<T> opened = Collections.synchronizedList(new LinkedList<T>());
	/** The timeout to wait for a connection from the pool **/
	protected long timeout = 3;
	/** Minimum number of connections in the pool **/
	protected int minConnections = 4;
	/** Maximum number of connections in the pool **/
	protected int maxConnections = 32;

	public DatabaseConnectionPool(DatabaseConnectionFactory<T> factory, Properties properties)
	{
		this.factory = factory;
		this.minConnections = Integer.parseInt(properties.getProperty("pool.minConnections", "4"));
		this.maxConnections = Integer.parseInt(properties.getProperty("pool.maxConnections", "32"));
	}
	
	public String toString()
	{
		StringBuilder msg = new StringBuilder();
		msg.append("DatabaseConnectionPool[min: ");
		msg.append(Integer.toString(minConnections));
		msg.append(", opened: ");
		msg.append(Integer.toString(opened.size()));
		msg.append(", inpool: ");
		msg.append(Integer.toString(pool.size()));
		msg.append(", max: ");
		msg.append(Integer.toString(maxConnections));
		msg.append("]");
		return msg.toString();
	}
	
	/**
	 * Retrieves a connection from the pool.
	 * There is a suppressed null warning here because the warning is caused by a bug in the compiler.
	 * @return
	 */
	@SuppressWarnings("null")
	public T getConnection()
	{
		if (!closed)
		{
			long requestTime = System.currentTimeMillis();
			T result = null;
			
			do
			{
				try
				{
					result = pool.poll(timeout, TimeUnit.SECONDS);
					if (result == null) 
					{
						logger.warning(toString() + " Waited " + timeout + " seconds for an available connection!");
						if (size() < maxConnections)
						{
							long start = System.currentTimeMillis();
							result = factory.createConnection();
							long finish = System.currentTimeMillis();
							long expired = ((finish - start) / 1000);
							logger.warning(toString() + " Created new connection (" + expired + " seconds)");
						}
						else 
						{
							logger.warning(toString() + " Pool at maximum size, refusing to create new connection!");
							long current = System.currentTimeMillis();
							for (T con : opened)
							{
								long duration = ((current - con.getAcquiredTime()) / 1000); 
								if (duration >= timeout)
								{
									logger.warning("Connection " + con.getConnectionId() + " exceeded open timeout: " + duration);
									StringBuilder msg = new StringBuilder();
									msg.append("Printing stack trace of opening thread:\n");
									StackTraceElement[] trace = con.getStackTrace();
									for (StackTraceElement element : trace)
									{
										msg.append(element.getClassName());
										msg.append('.');
										msg.append(element.getMethodName());
										msg.append("() ");
										msg.append(element.getFileName());
										msg.append(':');
										msg.append(element.getLineNumber());
										msg.append('\n');
									}
									logger.warning(msg.toString());
								}
							}
							continue;
						}
					}
					initializeConnection(result);
					result.unlock();
					opened.add(result);						
				}
				
				catch (Exception e)
				{
					e.printStackTrace();
					throw new DatabaseException(e);
				}
			}
			while (result == null);
			long responseTime = System.currentTimeMillis();
			long totalTime = ((responseTime - requestTime) / 1000);
			if ((totalTime > timeout) && (totalTime > 0))
			{
				logger.warning(toString() + " Connection returned in " + totalTime + " seconds.");
			}
			logger.warning("Providing connection " + result.getConnectionId());
			return result;			
		}
		else return null;
	}
	
	public int size()
	{
		return pool.size() + opened.size();
	}
	
	public void initializeConnection(T con)
	{
		con.setDatabaseConnectionPool(this);
		con.setAcquiredTime(System.currentTimeMillis());
		try { throw new Exception("StackTrace Collection Exception"); }
		catch (Exception e) { con.setStackTrace(e.getStackTrace());} 
	}
	
	public void putConnection(T con)
	{
		long current = System.currentTimeMillis();
		long duration = ((current - con.getAcquiredTime()) / 1000);
		if ((duration > timeout) && (duration > 0))
		{
			logger.warning("Returned connection " + con.getConnectionId() + " to pool after " + duration + " seconds.");
		}
		con.setAcquiredTime(0);		
		con.setStackTrace(null);
		con.setDatabaseConnectionPool(this);
		opened.remove(con);
		con.lock();
		pool.add(con);
	}
	
	public void open()
	{
		closed = false;
		for (int i = 0; i < minConnections; i++)
		{
			T con = factory.createConnection();
			con.setAcquiredTime(System.currentTimeMillis());
			putConnection(con);
		}
	}
	
	public void close()
	{
		closed = true;
		for (T con : pool)
		{
			con.destroy();
		}
		pool.clear();
		
		for (T con : opened)
		{
			con.destroy();
		}
		opened.clear();
	}

	protected void finalize()
	{
		close();
	}
}
