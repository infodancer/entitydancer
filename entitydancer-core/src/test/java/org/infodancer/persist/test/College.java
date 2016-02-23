package org.infodancer.persist.test;

import java.util.Collection;
import java.util.HashSet;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * This class is used to test bidirectional OneToMany relationships.
 * @author matthew
 */
@Entity
public class College 
{
	private Long id;
	private String name;
	private Collection<Student> students = new HashSet<Student>();
	
	@Id
	@GeneratedValue
	public Long getId()
	{
		return id;
	}
	
	public void setId(Long id)
	{
		this.id = id;
	}
	
	@Column(length=80)
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	@OneToMany(mappedBy="college")
	public Collection<Student> getStudents()
	{
		return students;
	}
	
	public void setStudents(Collection<Student> students)
	{
		this.students = students;
	}
}
