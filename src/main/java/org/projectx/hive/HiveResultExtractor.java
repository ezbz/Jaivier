package org.projectx.hive;

import java.util.List;

public interface HiveResultExtractor<T> {

  List<T> extractData(List<String> result);

}
