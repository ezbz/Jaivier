package org.projectx.hive;

/**
 * A {@link RowMapper} which simply returns the raw line
 * @author erez
 *
 */
public class SimpleRowMapper implements RowMapper<String> {

  @Override
  public String mapRow(final String row, final int rowNum) {
    return row;
  }

}
