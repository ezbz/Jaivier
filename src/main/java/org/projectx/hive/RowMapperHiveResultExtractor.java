package org.projectx.hive;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

public class RowMapperHiveResultExtractor<T> implements HiveResultExtractor<T> {

  private final RowMapper<T> rowMapper;

  private final int rowsExpected;

  public RowMapperHiveResultExtractor(final RowMapper<T> rowMapper) {
    this(rowMapper, 0);
  }

  public RowMapperHiveResultExtractor(final RowMapper<T> rowMapper, final int rowsExpected) {
    Assert.notNull(rowMapper, "RowMapper is required");
    this.rowMapper = rowMapper;
    this.rowsExpected = rowsExpected;
  }

  @Override
  public List<T> extractData(final List<String> hiveResult) {
    final List<T> results = (this.rowsExpected > 0 ? new ArrayList<T>(this.rowsExpected)
                                                  : new ArrayList<T>());
    int rowNum = 0;
    for (final String result : hiveResult) {
      results.add(rowMapper.mapRow(result, rowNum++));
    }
    return results;
  }
}
