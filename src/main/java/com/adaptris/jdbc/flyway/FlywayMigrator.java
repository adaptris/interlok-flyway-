package com.adaptris.jdbc.flyway;

import javax.sql.DataSource;

@FunctionalInterface
public interface FlywayMigrator {

  void migrate(DataSource ds) throws Exception;
}
