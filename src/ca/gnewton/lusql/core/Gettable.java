package ca.gnewton.lusql.core;

public interface Gettable
{
	public Doc get(String field, String key) throws DataSourceException;
}
