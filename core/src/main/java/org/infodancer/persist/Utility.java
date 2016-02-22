package org.infodancer.persist;

public class Utility 
{
	public static int parseAnnotationInteger(int value, int defaultValue)
	{
		if (value != 0) return value;
		else return defaultValue;
	}

	public static String parseAnnotationString(String value, String defaultValue)
	{
		if (value != null)
		{
			if (value.trim().length() > 0)
			{
				return value;
			}
		}
		return defaultValue;
	}
}
