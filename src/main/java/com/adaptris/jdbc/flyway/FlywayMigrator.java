package com.adaptris.jdbc.flyway;

import javax.sql.DataSource;

import org.apache.commons.lang3.ObjectUtils;

@FunctionalInterface
public interface FlywayMigrator {

  void migrate(DataSource ds) throws Exception;

  static FlywayMigrator defaultIfNull(FlywayMigrator migrator) {
    return ObjectUtils.defaultIfNull(migrator, (ds) -> {
    });
  }

}
