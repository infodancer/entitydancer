package org.infodancer.persist.test;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.Query;

import junit.framework.TestCase;

import org.infodancer.persist.DatabaseRelationshipCollection;
import org.infodancer.persist.EntityType;
import org.infodancer.persist.ServiceEntityManager;

/**
 * Abstract class providing test cases for entitymanagers.
 * @author matthew
 *
 */
public abstract class EntityManagerTest extends TestCase
{
	private static final Logger logger = Logger.getLogger("EntityManagerTest");
	protected ServiceEntityManager manager;

	public void setEntityManager(ServiceEntityManager manager)
	{
		this.manager = manager;
	}
	
	public void testIsModified() 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, InstantiationException
	{
		EntityTest test1 = new EntityTest();
		test1.setBooleanValue(true);
		manager.persist(test1);
		assertTrue(manager.contains(test1));
		// Populate the cache...
		EntityTest test2 = manager.find(EntityTest.class, test1.getId());
		assertTrue(!manager.isModified(test1));
		test1.setBooleanValue(false);
		assertTrue(manager.isModified(test1));
	}
	
	public void testClose()
	{
		assertTrue(manager.isOpen());
		manager.close();
		assertFalse(manager.isOpen());
	}
	
	public void testClear()
	{
		// Not tested since no caching enabled at present
	}
	
	public void testFlush()
	{
		// Not tested since no caching enabled at present
	}
	
	public void testGetTransaction()
	{
		EntityTransaction transaction = manager.getTransaction();
		assertNotNull(transaction);
	}
	
	public void testGetFlushMode()
	{
		manager.setFlushMode(FlushModeType.AUTO);
		assertEquals(FlushModeType.AUTO, manager.getFlushMode());
		manager.setFlushMode(FlushModeType.COMMIT);
		assertEquals(FlushModeType.COMMIT, manager.getFlushMode());
	}
	
	public void testRemove()
	{
		EntityTest test = new EntityTest();
		manager.persist(test);
		assertTrue(manager.contains(test));
		manager.remove(test);
		assertFalse(manager.contains(test));
	}
	
	public void testIsSupportedFieldType()
	{
		List<Class> supported = new ArrayList<Class>();
		supported.add(Trinary.class);
		supported.add(String.class);
		supported.add(Integer.class);
		supported.add(int.class);
		supported.add(Long.class);
		supported.add(long.class);
		supported.add(Float.class);
		supported.add(float.class);
		supported.add(Double.class);
		supported.add(double.class);
		supported.add(Byte[].class);
		supported.add(byte[].class);
		supported.add(Boolean.class);
		supported.add(boolean.class);
		supported.add(java.util.Date.class);
		supported.add(java.sql.Date.class);
		supported.add(java.sql.Time.class);
		supported.add(java.sql.Timestamp.class);
		supported.add(java.util.Collection.class);
		supported.add(java.util.Set.class);
		
		for (Class c : supported)
		{
			assertTrue(c.toString() + " should be a supported type!", manager.isSupportedFieldType(c));
		}
	}
	
	public void testPersistEnum()
	{
		// Set up one of each trinary value
		EnumTest p1 = new EnumTest();
		p1.setTrinary(Trinary.NO);
		manager.persist(p1);
		EnumTest p2 = new EnumTest();
		p2.setTrinary(Trinary.MAYBE);
		manager.persist(p2);
		EnumTest p3 = new EnumTest();
		p3.setTrinary(Trinary.YES);
		manager.persist(p3);
		
		// Make sure they all persisted
		assertTrue(manager.contains(p1));
		assertTrue(manager.contains(p2));
		assertTrue(manager.contains(p3));
		
		// Make sure the values are retrieved
		EnumTest f1 = manager.find(EnumTest.class, p1.getId());
		assertNotNull(f1);
		EnumTest f2 = manager.find(EnumTest.class, p2.getId());
		assertNotNull(f2);		
		EnumTest f3 = manager.find(EnumTest.class, p3.getId());
		assertNotNull(f3);
		
		// Make sure the values match
		assertEquals(p1.getTrinary(), f1.getTrinary());
		assertEquals(p2.getTrinary(), f2.getTrinary());
		assertEquals(p3.getTrinary(), f3.getTrinary());
	}
	
	public void testPersist()
	{
		EntityTest test1 = new EntityTest();
		test1.setIntValue(1);
		test1.setFloatValue(2.0f);
		test1.setDoubleValue(3.0);
		test1.setName("Name");
		test1.setTrinaryValue(Trinary.MAYBE);
		test1.setBooleanValue(true);
		manager.persist(test1);
		assertTrue(manager.contains(test1));
		EntityTest test2 = manager.find(EntityTest.class, test1.getId());
		assertNotNull(test2);
		assertEquals(test1.getId(), test2.getId());
		assertEquals(test1.getName(), test2.getName());
		assertEquals(test1.getDoubleValue(), test2.getDoubleValue());
		assertEquals(test1.getFloatValue(), test2.getFloatValue());
		assertEquals(test1.getIntValue(), test2.getIntValue());
		assertEquals(test1.getTrinaryValue(), test2.getTrinaryValue());
		assertEquals(test1.isBooleanValue(), test2.isBooleanValue());
	}
	
	public void testOneToOnePersistFind()
	{
		OneToOneEntity entityOne = new OneToOneEntity();
		entityOne.setName("TestingOneToOneEntity");
		AnotherEntity anotherOne = new AnotherEntity();
		anotherOne.setName("TestingAnotherEntity");
		entityOne.setAnotherEntity(anotherOne);
		manager.persist(entityOne);
		OneToOneEntity entityTwo = manager.find(OneToOneEntity.class, entityOne.getId());
		assertNotNull(entityTwo);
		assertEquals(entityOne.getId(), entityTwo.getId());
		assertEquals(entityOne.getName(), entityTwo.getName());
		AnotherEntity anotherTwo = entityTwo.getAnotherEntity();
		assertEquals(anotherTwo.getId(), anotherTwo.getId());
		assertEquals(anotherTwo.getName(), anotherTwo.getName());
	}
	
	public void testOneToOneMerge()
	{
		OneToOneEntity entityOne = new OneToOneEntity();
		entityOne.setName("TestingOneToOneEntity");
		AnotherEntity anotherOne = new AnotherEntity();
		anotherOne.setName("TestingAnotherEntity");
		entityOne.setAnotherEntity(null);
		manager.persist(entityOne);
		entityOne.setName("TestingOneToOneEntityModified");
		anotherOne.setName("TestingAnotherOneModified");
		entityOne.setAnotherEntity(anotherOne);
		manager.merge(entityOne);
		OneToOneEntity entityTwo = manager.find(OneToOneEntity.class, entityOne.getId());
		assertNotNull(entityTwo);
		assertEquals(entityOne.getId(), entityTwo.getId());
		assertEquals(entityOne.getName(), entityTwo.getName());
		AnotherEntity anotherTwo = entityTwo.getAnotherEntity();
		assertEquals(anotherTwo.getId(), anotherTwo.getId());
		assertEquals(anotherTwo.getName(), anotherTwo.getName());
	}
	
	public void testRefresh()
	{
		String oldname = "oldname";
		String newname = "newname";
		EntityTest test = new EntityTest();
		test.setName(oldname);
		manager.persist(test);
		test.setName(newname);
		manager.refresh(test);
		assertEquals(oldname, test.getName());
	}
		
	public void testMerge()
	{
		String testName1 = "Test1";
		String testName2 = "Test2";
		
		EntityTest test1 = new EntityTest();
		test1.setName(testName1);
		assertFalse(manager.contains(test1));
		manager.persist(test1);
		assertTrue(manager.contains(test1));
		EntityTest test2 = manager.find(EntityTest.class, test1.getId());
		assertNotNull(test2);
		test2.setName(testName2);
		manager.merge(test2);
		EntityTest test3 = manager.find(EntityTest.class, test2.getId());
		assertNotNull(test3);
		assertEquals(test2.getName(), test3.getName());
	}
	
	public void testIsOpen()
	{
		assertTrue(manager.isOpen());
		manager.close();
		assertFalse(manager.isOpen());
	}
	
	public void testSetFlushMode()
	{
		FlushModeType mode = null;
		manager.setFlushMode(FlushModeType.AUTO);
		mode = manager.getFlushMode();
		assertEquals(FlushModeType.AUTO, mode);
		manager.setFlushMode(FlushModeType.COMMIT);
		mode = manager.getFlushMode();
		assertEquals(FlushModeType.COMMIT, mode);
	}
	
	public void testJoinTransaction()
	{
	}
	
	public void testFind()
	{
		EntityTest test = new EntityTest();
		assertFalse(manager.contains(test));
		manager.persist(test);
		assertTrue(manager.contains(test));
		EntityTest test2 = manager.find(EntityTest.class, test.getId());
		assertNotNull(test2);
	}
	
	public void testContains()
	{
		EntityTest test = new EntityTest();
		assertFalse(manager.contains(test));
		manager.persist(test);
		assertTrue(manager.contains(test));
	}
	
	public void testLock()
	{
	}
	
	public void testBiManyToOnePersist()
	{
		Department department = new Department();
		department.setName("Dancers");
		manager.persist(department);
		Employee employee = new Employee();
		employee.setFirstName("Peaches");
		employee.setLastName("Cream");
		employee.setDepartment(department);
		manager.persist(employee);
		Employee employee1 = manager.find(Employee.class, employee.getId());
		assertNotNull(employee1);
		assertNotNull(employee1.getDepartment());
		assertEquals(employee.getFirstName(), employee1.getFirstName());
		assertEquals(employee.getLastName(), employee1.getLastName());
	}

	public void testUniManyToManyPersist()
	{
		House house = new House();
		Resident resident1 = new Resident();
		Resident resident2 = new Resident();
		Collection<Resident> residents = house.getResidents();
		residents.add(resident1);
		residents.add(resident2);
		manager.persist(house);		
		assertNotNull(house.getId());
		assertNotNull(resident1.getId());
		assertNotNull(resident2.getId());
		assertTrue(manager.contains(house));
		assertTrue(manager.contains(resident1));
		assertTrue(manager.contains(resident2));
		House nhouse = manager.find(House.class, house.getId());
		assertNotNull(nhouse);
		assertNotNull(nhouse.getId());
		assertEquals(house.getId(), nhouse.getId());
		Collection<Resident> nresidents = nhouse.getResidents();
		if (!(nresidents instanceof DatabaseRelationshipCollection)) 
		{
			fail("nresidents must be an instance of DatabaseRelationshipCollection!");
		}
	}

	public void testUniOneToManyPersist()
	{
		DoorLock lock = new DoorLock();
		lock.setName("My Test Lock");
		DoorKey key1 = new DoorKey();
		DoorKey key2 = new DoorKey();
		Collection<DoorKey> keys = lock.getKeys();
		keys.add(key1);
		keys.add(key2);
		manager.persist(lock);
		assertTrue(manager.contains(lock));
		assertTrue(manager.contains(key1));
		assertTrue(manager.contains(key2));
	}
	
	public void testBiManyToManyPersist()
	{
		Car car1 = new Car();
		Car car2 = new Car();
		Car car3 = new Car();
		Driver driver1 = new Driver();
		Driver driver2 = new Driver();
		Driver driver3 = new Driver();
		Collection<Driver> drivers1 = car1.getDrivers();
		drivers1.add(driver1);
		Collection<Driver> drivers2 = car2.getDrivers();
		drivers2.add(driver1);
		drivers2.add(driver2);
		Collection<Driver> drivers3 = car3.getDrivers();
		drivers3.add(driver1);
		drivers3.add(driver2);
		drivers3.add(driver3);
		manager.persist(car1);
		manager.persist(car2);
		manager.persist(car3);
		assertTrue(manager.contains(car1));
		assertTrue(manager.contains(car2));
		assertTrue(manager.contains(car3));
		assertTrue(manager.contains(driver1));
		assertTrue(manager.contains(driver2));
		assertTrue(manager.contains(driver3));
	}
	
	public void testBiOneToManyPersist()
	{
		College college1 = new College();
		college1.setName("Carnegie Mellon University");
		College college2 = new College();
		college2.setName("University of Pittsburgh");
		
		Student student1 = new Student();
		student1.setFirstName("Matthew");
		student1.setLastName("Smith");
		Student student2 = new Student();
		student2.setFirstName("Sarah");
		student2.setLastName("Smith");
		Student student3 = new Student();
		student3.setFirstName("Michelle");
		student3.setLastName("Smith");
		student1.setCollege(college1);
		student2.setCollege(college2);
		student3.setCollege(college2);
		manager.persist(college1);
		assertNotNull(college1.getId());
		manager.persist(college2);
		assertNotNull(college2.getId());
		manager.persist(student1);
		assertNotNull(student1.getId());
		manager.persist(student2);
		assertNotNull(student2.getId());
		manager.persist(student3);
		assertNotNull(student3.getId());
		assertNotSame(student1.getId(), student2.getId());
		assertNotSame(student1.getId(), student3.getId());
		assertNotSame(student2.getId(), student3.getId());
		assertSame(student1.getCollege(), college1);
		assertSame(student2.getCollege(), college2);
		assertSame(student3.getCollege(), college2);
		assertTrue(manager.contains(college1));
		assertTrue(manager.contains(college2));
		assertTrue(manager.contains(student1));
		assertTrue(manager.contains(student2));
		assertTrue(manager.contains(student3));
		Object ckey1 = college1.getId();
		Object ckey2 = college2.getId();
		college1 = manager.find(College.class, ckey1);
		college2 = manager.find(College.class, ckey2);
		Collection<Student> c1students = college1.getStudents();
		assertTrue(c1students.contains(student1));
		assertFalse(c1students.contains(student2));
		assertFalse(c1students.contains(student3));
		Collection<Student> c2students = college2.getStudents();
		assertFalse(c2students.contains(student1));
		assertTrue(c2students.contains(student2));
		assertTrue(c2students.contains(student3));
	}

	/**
	 * Tests for basic query functionality.
	 */

	public void testQuery()
	{
		// Reuse the setup from testBiOneToManyPersist because it is handy
		College college1 = new College();
		college1.setName("Carnegie Mellon University");
		College college2 = new College();
		college2.setName("University of Pittsburgh");
		Student student1 = new Student();
		student1.setFirstName("Matthew");
		student1.setLastName("Smith");
		Student student2 = new Student();
		student2.setFirstName("Sarah");
		student2.setLastName("Smith");
		Student student3 = new Student();
		student3.setFirstName("Michelle");
		student3.setLastName("Smith");
		student1.setCollege(college1);
		student2.setCollege(college2);
		student3.setCollege(college2);
		manager.persist(college1);
		assertNotNull(college1.getId());
		manager.persist(college2);
		assertNotNull(college2.getId());
		manager.persist(student1);
		assertNotNull(student1.getId());
		manager.persist(student2);
		assertNotNull(student2.getId());
		manager.persist(student3);
		assertNotNull(student3.getId());
		assertNotSame(student1.getId(), student2.getId());
		assertNotSame(student1.getId(), student3.getId());
		assertNotSame(student2.getId(), student3.getId());
		assertSame(student1.getCollege(), college1);
		assertSame(student2.getCollege(), college2);
		assertSame(student3.getCollege(), college2);
		assertTrue(manager.contains(college1));
		assertTrue(manager.contains(college2));
		assertTrue(manager.contains(student1));
		assertTrue(manager.contains(student2));
		assertTrue(manager.contains(student3));
		// Now here's the new query tests
		Query query = manager.createQuery("SELECT c FROM College c");
		List<College> colleges = (List<College>) query.getResultList();
		assertEquals(colleges.size(), 2);
	}

	protected void initializeSimpleQueryTest()
	{
		Address address = new Address();
		address.setStreet("123 Easy Street");
		Customer customer = new Customer();
		customer.setDescription("This is a happy customer.");
		customer.setAddress(address);
		Purchase order1 = new Purchase();
		order1.setQuantity(13);
		order1.setCustomer(customer);
		order1.setItemName("BigRedBalloon");
		manager.persist(order1);
		Purchase order2 = new Purchase();
		order2.setQuantity(3);
		order2.setCustomer(customer);
		order2.setItemName("BigGreenBalloon");
		manager.persist(order2);
	}
	
	public void testSimpleQuery1()
	{
		initializeSimpleQueryTest();
		Query query = manager.createQuery("SELECT o FROM Purchase o");
		@SuppressWarnings("unchecked")
		List<Purchase> orders = (List<Purchase>) query.getResultList();
		for (Purchase order : orders)
		{
			int quantity = order.getQuantity();
			String item = order.getItemName();
			assertNotNull(item);
			if ("BigRedBalloon".equals(item))
			{
				assertEquals(13, quantity);
			}
			else if ("BigGreenBalloon".equals(item))
			{
				assertEquals(3, quantity);
			}
		}
	}
	
	public void testSimpleWhereQuery1()
	{
		initializeSimpleQueryTest();
		StringBuilder result = new StringBuilder();
		result.append("SELECT o FROM Purchase o");
		result.append(" WHERE o.itemName = 'BigRedBalloon'");
		Query query = manager.createQuery(result.toString());
		List<Purchase> orders = (List<Purchase>) query.getResultList();
		for (Purchase order : orders)
		{
			assertEquals("BigRedBalloon", order.getItemName());
		}
	}

	public void testSimpleWhereQuery2()
	{
		initializeSimpleQueryTest();
		StringBuilder result = new StringBuilder();
		result.append("SELECT o FROM Purchase o");
		result.append(" WHERE o.quantity = 13");
		Query query = manager.createQuery(result.toString());
		List<Purchase> orders = (List<Purchase>) query.getResultList();
		for (Purchase order : orders)
		{
			assertEquals(13, order.getQuantity());
		}
	}

	public void testSimpleWhereLessThanQuery()
	{
		initializeSimpleQueryTest();
		StringBuilder result = new StringBuilder();
		result.append("SELECT o FROM Purchase o");
		result.append(" WHERE o.quantity < 13");
		Query query = manager.createQuery(result.toString());
		List<Purchase> orders = (List<Purchase>) query.getResultList();
		for (Purchase order : orders)
		{
			assertEquals(3, order.getQuantity());
		}
	}

	public void testSimpleWhereGreaterThanQuery()
	{
		initializeSimpleQueryTest();
		StringBuilder result = new StringBuilder();
		result.append("SELECT o FROM Purchase o");
		result.append(" WHERE o.quantity > 3");
		Query query = manager.createQuery(result.toString());
		@SuppressWarnings("unchecked")
		List<Purchase> orders = (List<Purchase>) query.getResultList();
		for (Purchase order : orders)
		{
			assertEquals(13, order.getQuantity());
		}
	}
	
	public void testSinglePersistSpeed()
	{
		long start = 0, finish = 0;
		EntityTest test = new EntityTest();
		test.setName("EntityTestObject");
		test.setDoubleValue((double) 1.0);
		test.setFloatValue((float) 2.0);
		test.setIntValue(3);
		test.setTrinaryValue(Trinary.MAYBE);
		start = System.nanoTime();
		manager.persist(test);
		finish = System.nanoTime();
		long time = finish - start;
		System.out.println("testSinglePersistSpeed: " + time + " in nanoseconds");
	}

	public void testMultiplePersistSpeed()
	{
		long start = 0, finish = 0;
		EntityTest[] tests = new EntityTest[1000];
		// Create 1000 EntityTest objects
		for (int i = 0; i < tests.length; i++)
		{
			tests[i] = new EntityTest();
			tests[i].setName("EntityTestObject " + i);
			tests[i].setDoubleValue((double) i);
			tests[i].setFloatValue((float) i);
			tests[i].setIntValue(i);
			tests[i].setTrinaryValue(Trinary.MAYBE);
		}
		// Store all of the objects
		start = System.nanoTime();
		for (int i = 0; i < tests.length; i++)
		{
			manager.persist(tests[i]);
		}
		// Flush the cache to ensure we are actually testing persist time
		manager.flush();
		finish = System.nanoTime();
		long time = finish - start;
		System.out.println("testMultiplePersistSpeed: " + time + " in nanoseconds");
	}

	public void testBlobLength() throws SQLException
	{
		byte[] value = null;
		BlobEntity eb1 = null, eb2 = null;
		Blob blob1 = null, blob2 = null;

		try
		{
			String svalue = "This is a test blob value";
			value = svalue.getBytes();
			eb1 = new BlobEntity();
			manager.persist(eb1);
			eb1 = manager.find(BlobEntity.class, eb1.getId());
			blob1 = eb1.getBinaryValue();
			blob1.setBytes(1, value);
			assertEquals(value.length, blob1.length());
		}
		
		finally
		{
			if (blob1 != null) blob1.free();	
		}
		
		try
		{
			eb2	= manager.find(BlobEntity.class, eb1.getId());
			blob2 = eb2.getBinaryValue();
			assertEquals(value.length, blob2.length());
			assertEquals(blob1.length(), blob2.length());	
		}

		finally
		{
			if (blob2 != null) blob2.free();
		}
	}

	public void testBlobBytes() throws SQLException
	{
		Blob blob1 = null;
		Blob blob2 = null;

		try
		{
			String svalue = "This is a test blob value";
			byte[] value = svalue.getBytes();
			BlobEntity eb1 = new BlobEntity();
			manager.persist(eb1);
			eb1 = manager.find(BlobEntity.class, eb1.getId());
			blob1 = eb1.getBinaryValue();
			blob1.setBytes(1, value);
			BlobEntity eb2 = manager.find(BlobEntity.class, eb1.getId());
			blob2 = eb2.getBinaryValue();
			assertEquals(new String(value), new String(blob2.getBytes(1, value.length)));
		}

		finally
		{
			if (blob1 != null) blob1.free();
			if (blob2 != null) blob2.free();
		}
	}
	
	public void testBlobStream() throws SQLException, IOException
	{
		Blob blob1 = null;
		Blob blob2 = null;
		
		try
		{
			String svalue = "This is a test blob value";
			byte[] value = svalue.getBytes();
			BlobEntity eb1 = new BlobEntity();
			manager.persist(eb1);
			eb1 = manager.find(BlobEntity.class, eb1.getId());
			blob1 = eb1.getBinaryValue();
			OutputStream output = blob1.setBinaryStream(1);
			output.write(value);
			output.close();
			BlobEntity eb2 = manager.find(BlobEntity.class, eb1.getId());
			blob2 = eb2.getBinaryValue();
			InputStream input = blob2.getBinaryStream();
			byte[] value2 = new byte[value.length];
			input.read(value2);
			input.close();
			assertEquals(new String(value), new String(value2));
		}
		
		finally
		{
			if (blob1 != null) blob1.free();
			if (blob2 != null) blob2.free();
		}
	}
	
	/*
	public void testSimpleQuery3()
	{
		initializeSimpleQueryTest();
		Query query = manager.createQuery("SELECT o FROM Purchase o WHERE o.shippingAddress.state = 'CA'");
	}
	*/ 
	
	/*
	public void testSimpleQuery3()
	{
		initializeSimpleQueryTest();
		Query query = manager.createQuery("SELECT DISTINCT o.shippingAddress.state FROM Order");
	}
	*/
	/*
	public void testCreateNamedQuery()
	{
		Query query = manager.createNamedQuery("");
		assertNotNull(query);
	}
	
	public void testCreateNativeQuery()
	{
		Query query = manager.createNativeQuery("");
		assertNotNull(query);
	}	
	*/
	
	public void testLoadCache() throws IntrospectionException, ClassNotFoundException
	{
		manager.createCacheTable();
		EntityType type1 = manager.getEntity(EntityTest.class);
		EntityType type2 = manager.getEntity(House.class);
		EntityType type3 = manager.getEntity(Purchase.class);
		List<Class> classes = manager.loadCache();
		assertEquals(7, classes.size());
	}
}
