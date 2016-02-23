package org.infodancer.persist;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.persistence.PersistenceException;
import javax.persistence.Table;

public class EntityType
{
	private static final Logger logger = Logger.getLogger("EntityType");
	protected long cachetry = 0;
	protected long cachehit = 0;
	protected long cachemiss = 0;
	protected Map<Object, WeakReference<Object>> cache;
	protected Class type;
	protected BeanInfo info;
	protected Annotation[] annotations;
	protected ServiceEntityManager manager;
	protected String className;
	protected String tableName;
	protected EntityField key;
	protected List<EntityField> fields;	
	
	public EntityType(ServiceEntityManager manager, Class type) 
	throws IntrospectionException, ClassNotFoundException 
	{
		this.type = type;
		this.info = Introspector.getBeanInfo(type);
		this.manager = manager;
 		this.className = type.getSimpleName();
		this.tableName = type.getSimpleName();
		this.annotations = type.getAnnotations();
		this.fields = new ArrayList<EntityField>();
		this.cache = new ConcurrentHashMap<Object,WeakReference<Object>>();
		initialize();
		if (key == null)
		{
			StringBuilder msg = new StringBuilder();
			msg.append(className);
			msg.append(" has no primary key!");
			throw new PersistenceException(msg.toString());
		}
	}

	public String getClassName()
	{
		return className;
	}
	
	protected void initialize() throws IntrospectionException, ClassNotFoundException
	{
		initialize(annotations);
		initialize(info.getPropertyDescriptors());
	}

	/**
	 * Examine the entity annotations.
	 * @param annotations
	 */
	protected void initialize(Annotation[] annotations)
	{
		for (Annotation annotation : annotations)
		{
			if (annotation instanceof Table)
			{
				Table table = (Table) annotation;
				tableName = Utility.parseAnnotationString(table.name(), tableName);
			}
		}
	}

	/**
	 * Examine the annotations on a property.
	 * @param properties
	 * @throws IntrospectionException 
	 * @throws ClassNotFoundException 
	 */
	protected void initialize(PropertyDescriptor[] properties) 
	throws IntrospectionException, ClassNotFoundException
	{
 		for (PropertyDescriptor property : properties)
		{
			logger.finest("Parsing annotations for property: " + property.getName());
			if (!isTransientProperty(property))
			{
				if (manager.isSupportedFieldType(property.getPropertyType()) || isManyToOneProperty(property))
				{
					EntityField field = new EntityField(manager, this, property);
					if (field.isPrimaryKey()) key = field; 
					else fields.add(field);
				}
			}
		}		
	}
	
	/**
	 * Retrieves the list of fields.
	 * Provides for type-safe implementation by subclasses.
	 * @return
	 */
	public List<EntityField> getFields()
	{
		return fields;
	}

	/**
	 * Retrieves the list of fields.
	 * Provides for type-safe implementation by subclasses.
	 * Matching is not case-sensitive for ease of use in SQL.
	 * @return
	 */
	public EntityField getFieldByJavaName(String name)
	{
		if (name != null)
		{
			for (EntityField field : fields)
			{
				String fieldName = field.getName();
				if (name.equalsIgnoreCase(fieldName)) return field;
			}
			return null;
		}
		else throw new PersistenceException("getFieldByJavaName() called with null parameter!");
	}

	/**
	 * Retrieves the list of fields.
	 * Provides for type-safe implementation by subclasses.
	 * @return
	 */
	public EntityField getFieldByStoreName(String name) 
	{
		if (name != null)
		{
			try
			{
				for (EntityField field : fields)
				{
					String fieldName = field.getStoreName();
					if (name.equalsIgnoreCase(fieldName)) return field;
				}
				return null;
			}
			
			catch (Exception e) 
			{
				throw new PersistenceException("Error retrieving field " + name);
			}
		}
		else throw new PersistenceException("getFieldByStoreName() called with null parameter!");
	}
		
	public void setPrimaryKeyValue(Object o, Object value) 
	{
		try
		{
			key.setFieldValue(o, value);
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException("Error setting primary key field value!");
		}
	}
	
	/**
	 * Retrieves the primary key field for this Entity.
	 * @return
	 */
	public EntityField getPrimaryKey()
	{
		return key;
	}
		
	private boolean isTransientProperty(PropertyDescriptor info) 
	{
		if (info != null)
		{
			Method method = info.getReadMethod();
			if (method != null)
			{
				Annotation[] annotations = method.getAnnotations();
				return isTransient(annotations);
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append(className);
				msg.append(":");
				msg.append(info.getName());
				msg.append(" No read method available!");
				throw new PersistenceException(msg.toString());				
			}
		}
		else 
		{
			StringBuilder msg = new StringBuilder();
			msg.append(className);
			msg.append(" No PropertyDescriptor available!");
			throw new PersistenceException(msg.toString());			
		}
	}

	private boolean isManyToOneProperty(PropertyDescriptor info) 
	{
		if (info != null)
		{
			Method method = info.getReadMethod();
			if (method != null)
			{
				Annotation[] annotations = method.getAnnotations();
				return isManyToOne(annotations);
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append(className);
				msg.append(":");
				msg.append(info.getName());
				msg.append(" No read method available!");
				throw new PersistenceException(msg.toString());
			}
		}
		else 
		{
			StringBuilder msg = new StringBuilder();
			msg.append(className);
			msg.append(" No PropertyDescriptor available!");
			throw new PersistenceException(msg.toString());
		}
	}

	private boolean isTransient(Annotation[] annotations) 
	{
		for (int ii = 0; ii < annotations.length; ii++)
		{
			if (annotations[ii] instanceof javax.persistence.Transient)
			{
				return true;
			}
		}
		return false;
	}

	private boolean isManyToOne(Annotation[] annotations) 
	{
		for (int ii = 0; ii < annotations.length; ii++)
		{
			if (annotations[ii] instanceof javax.persistence.ManyToOne)
			{
				return true;
			}
		}
		return false;
	}

	public Object getPrimaryKeyValue(Object o) 
	{
		try
		{
			if (o != null) return key.getFieldValue(o); 
			else return null;
		}

		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException(e);
		}
	}
	
	public Class getEntityType()
	{
		return type;
	}
	
	/**
	 * Provides the table name for this entity.
	 * @return
	 */
	public String getTableName()
	{
		return tableName;
	}
	
	public Object newInstance() 
	{
		try
		{
			return type.newInstance();
		}
		
		catch (Exception e)
		{
			throw new PersistenceException(e);
		}
	}

	public Object getCache(Object keyvalue) 
	throws InstantiationException, IllegalAccessException
	{
		Object result = null;
		DecimalFormat format = new DecimalFormat();
		format.setMaximumFractionDigits(2);
		if (cachetry == Long.MAX_VALUE)
		{
			cachetry = 0;
			cachehit = 0;
			cachemiss = 0;
			logger.fine("Cache<" + className + ">: Cache statistics rolling over to 0");
		}
		cachetry++;
		WeakReference<Object> ref = cache.get(keyvalue);
		if (ref != null) 
		{
			result = ref.get();
			if (result == null) cachemiss++; 
			else cachehit++;
		}
		else cachemiss++;		
		if ((cachetry % 10000) == 0)
		{
			double rate = ((double) cachehit / cachetry);
			rate = rate * 100;
			logger.fine("Cache<" + className + ">: " + cachehit + " hits, " + cachemiss + " misses, " + cachetry + " tries, " + format.format(rate) + "% hit rate");
		}
		return clone(result);
	}
	
	public Object clone(Object o) throws InstantiationException, IllegalAccessException
	{
		if (o == null) return null;
		Object result = type.newInstance();
		Object key = getPrimaryKeyValue(o);
		setPrimaryKeyValue(result, key);
		for (EntityField field : getFields())
		{
			Object value = field.getFieldValue(o);
			field.setFieldValue(result, value);
		}
		return result;
	}

	public void putCache(Object keyvalue, Object entity)
	{
		if (entity == null) cache.remove(keyvalue); 
		else cache.put(keyvalue, new WeakReference<Object>(entity));
	}
}
