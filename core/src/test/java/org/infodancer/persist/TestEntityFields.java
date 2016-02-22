package org.infodancer.persist;

import java.beans.IntrospectionException;
import java.util.List;

import javax.persistence.PersistenceException;

import junit.framework.TestCase;

import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseTable;
import org.infodancer.persist.test.Address;
import org.infodancer.persist.test.AnnualReview;
import org.infodancer.persist.test.AnotherEntity;
import org.infodancer.persist.test.Car;
import org.infodancer.persist.test.College;
import org.infodancer.persist.test.Customer;
import org.infodancer.persist.test.DoorKey;
import org.infodancer.persist.test.DoorLock;
import org.infodancer.persist.test.Driver;
import org.infodancer.persist.test.Employee;
import org.infodancer.persist.test.House;
import org.infodancer.persist.test.OneToOneEntity;
import org.infodancer.persist.test.Purchase;
import org.infodancer.persist.test.Resident;
import org.infodancer.persist.test.Student;

public class TestEntityFields extends TestDatabaseOperations
{
	public void testAddressEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Address.class);
		assertNotNull(entity.getFieldByJavaName("version"));
		assertNotNull(entity.getFieldByJavaName("street"));
		List<EntityField> fields = entity.getFields();
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("Address".equalsIgnoreCase(tableName))
			{
				for (EntityField field : fields)
				{
					DatabaseField dbfield = table.getField(field.getName());
					assertNotNull(dbfield);
				}
			}
		}
	}

	public void testAnotherEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(AnotherEntity.class);
		assertNotNull(entity.getFieldByJavaName("name"));
		List<EntityField> fields = entity.getFields();
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("AnotherEntity".equalsIgnoreCase(tableName))
			{
				for (EntityField field : fields)
				{
					DatabaseField dbfield = table.getField(field.getName());
					assertNotNull(dbfield);
				}
			}
		}
	}

	public void testCarEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Car.class);
		assertNotNull(entity.getFieldByJavaName("year"));
		assertNotNull(entity.getFieldByJavaName("make"));
		assertNotNull(entity.getFieldByJavaName("model"));
		assertNotNull(entity.getFieldByJavaName("drivers"));
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("Car".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("year"));
				assertNotNull(table.getField("make"));
				assertNotNull(table.getField("model"));
			}
			else if ("Car_Driver".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("cars_id"));
				assertNotNull(table.getField("drivers_id"));				
			}
		}
	}

	public void testEmployeeEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Employee.class);
		assertNotNull(entity.getFieldByJavaName("firstName"));
		assertNotNull(entity.getFieldByJavaName("lastName"));
		assertNotNull(entity.getFieldByJavaName("department"));
		assertNotNull(entity.getFieldByJavaName("annualReviews"));
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("Employee".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("firstName"));
				assertNotNull(table.getField("lastName"));
				assertNotNull(table.getField("department_id"));
			}
			else if ("Employee_AnnualReview".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("Employee_id"));
				assertNotNull(table.getField("annualReviews_id"));				
			}
		}
	}

	public void testAnnualReviewEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(AnnualReview.class);
		assertNotNull(entity.getFieldByJavaName("rating"));
		assertNotNull(entity.getFieldByJavaName("comment"));
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("AnnualReview".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("rating"));
				assertNotNull(table.getField("comment"));
			}
			else throw new PersistenceException("Expected one table named AnnualReview!");
		}
	}

	public void testCollegeEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(College.class);
		assertNotNull(entity.getFieldByJavaName("name"));
		assertNotNull(entity.getFieldByJavaName("students"));
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String tableName = table.getName();
			if ("College".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("name"));
			}
			else if ("Student".equalsIgnoreCase(tableName))
			{
				assertNotNull(table.getField("college_id"));
			}
			else if ("College_Student".equalsIgnoreCase(tableName))
			{
				fail("This is a bidirectional relationship that does not need a join table!");
			}
			else if ("Student_College".equalsIgnoreCase(tableName))
			{
				fail("This is a bidirectional relationship that does not need a join table!");
			}
		}
	}

	public void testCustomerEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Customer.class);
		assertNotNull(entity.getFieldByJavaName("address"));
		assertNotNull(entity.getFieldByJavaName("description"));
		assertNotNull(entity.getFieldByJavaName("orders"));
		assertNotNull(entity.getFieldByJavaName("serviceOptions"));
		assertNotNull(entity.getFieldByJavaName("version"));
	}

	public void testDoorKeyEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(DoorKey.class);
		assertNotNull(entity.getFieldByJavaName("owner"));
	}
	
	public void testDoorLockEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(DoorLock.class);
		assertNotNull(entity.getFieldByJavaName("name"));
		assertNotNull(entity.getFieldByJavaName("keys"));
	}
	
	public void testDriverEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Driver.class);
		assertNotNull(entity.getFieldByJavaName("cars"));
		assertNotNull(entity.getFieldByJavaName("name"));
	}

	public void testHouseEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(House.class);
		assertNotNull(entity.getFieldByJavaName("address"));
		assertNotNull(entity.getFieldByJavaName("residents"));
	}

	/*
	public void testLineItemEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(LineItem.class);
		assertNotNull(entity.getFieldByJavaName(""));
		assertNotNull(entity.getFieldByJavaName(""));
	}
	*/
	
	public void testOneToOneEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(OneToOneEntity.class);
		assertNotNull(entity.getFieldByJavaName("anotherEntity"));
		assertNotNull(entity.getFieldByJavaName("name"));
		List<DatabaseTable> tables = manager.defineEntityStore(entity);
		for (DatabaseTable table : tables)
		{
			String name = table.getName();
			if ("OneToOneEntity".equalsIgnoreCase(name))
			{
				assertNotNull(table.getField("name"));
				assertNotNull(table.getField("anotherEntity_id"));
			}
		}
	}

	public void testPurchaseEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Purchase.class);
		assertNotNull(entity.getFieldByJavaName("customer"));
		assertNotNull(entity.getFieldByJavaName("itemName"));
		assertNotNull(entity.getFieldByJavaName("quantity"));
	}

	public void testResidentEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Resident.class);
		assertNotNull(entity.getFieldByJavaName("name"));
	}

	/*
	public void testShippingAddressEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(ShippingAddressEntity.class);
		assertNotNull(entity.getFieldByJavaName(""));
	}
	*/
	
	public void testStudentEntity() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(Student.class);
		assertNotNull(entity.getFieldByJavaName("firstName"));
		assertNotNull(entity.getFieldByJavaName("lastName"));
		assertNotNull(entity.getFieldByJavaName("college"));
	}

}

