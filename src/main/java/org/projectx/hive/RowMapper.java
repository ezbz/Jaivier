package org.projectx.hive;

public interface RowMapper<T> {

  T mapRow(String row, int rowNum);
}
