package com.adaptris.jdbc.flyway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.Test;

import com.adaptris.core.jdbc.C3P0PooledDataSource;
import com.adaptris.util.GuidGenerator;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class FlywayMigratorTest {

  private static final GuidGenerator GUID = new GuidGenerator();
  private static final String DRIVER_IMP = "org.apache.derby.jdbc.EmbeddedDriver";

  @Test
  public void testMigrate_Full() throws Exception {
    String jdbcUrl = "jdbc:derby:memory:" + GUID.safeUUID() + ";create=true";
    DefaultFlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(false).withFlywayLocations("classpath:migration/full");
    try (C3P0PooledDataSource source = new C3P0PooledDataSource(dataSource(jdbcUrl))) {
      migrator.migrate(source);
    }
    try (Connection db = connection(jdbcUrl)) {
      verifyCount(1, db);
    }
  }

  @Test
  public void testMigrate_Full_Table() throws Exception {
    String jdbcUrl = "jdbc:derby:memory:" + GUID.safeUUID() + ";create=true";
    String table = "alternative_flyway_schema_history";
    DefaultFlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(false).withFlywayLocations("classpath:migration/full")
        .withFlywayTable(table);
    try (C3P0PooledDataSource source = new C3P0PooledDataSource(dataSource(jdbcUrl))) {
      migrator.migrate(source);
    }
    try (Connection db = connection(jdbcUrl)) {
      verifyCount(1, db);
      verifyCount(1, db, table);
    }
  }

  // Expected to fail, since baseline = false.
  @Test
  public void testMigrate_Partial_NoBaseline() throws Exception {
    String jdbcUrl = "jdbc:derby:memory:" + GUID.safeUUID() + ";create=true";
    createUnbaselinedDatabase(jdbcUrl);
    DefaultFlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(false).withFlywayLocations("classpath:migration/partial");
    try (C3P0PooledDataSource source = new C3P0PooledDataSource(dataSource(jdbcUrl))) {
      assertThrows(FlywayException.class, () -> migrator.migrate(source));
    }
  }

  @Test
  public void testMigrate_Partial_Baseline() throws Exception {
    String jdbcUrl = "jdbc:derby:memory:" + GUID.safeUUID() + ";create=true";
    createUnbaselinedDatabase(jdbcUrl);
    DefaultFlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(true).withFlywayLocations("classpath:migration/partial");
    try (C3P0PooledDataSource source = new C3P0PooledDataSource(dataSource(jdbcUrl))) {
      migrator.migrate(source);
    }
    try (Connection db = connection(jdbcUrl)) {
      verifyCount(2, db);
    }
  }

  private void createUnbaselinedDatabase(String url) throws Exception {
    try (Connection db = connection(url); Statement stmt = db.createStatement()) {
      try {
        stmt.execute("DROP TABLE sequences");
      } catch (Exception e) {
      }
      stmt.execute("CREATE TABLE sequences " + "(id VARCHAR(255) NOT NULL, seq_number INT)");
      stmt.executeUpdate("INSERT INTO sequences (id, seq_number) values ('id', 2)");
    }
  }

  public static Connection connection(String url) throws Exception {
    Class.forName(DRIVER_IMP);
    Connection db = DriverManager.getConnection(url);
    db.setAutoCommit(true);
    return db;
  }

  public static ComboPooledDataSource dataSource(String url) throws Exception {
    ComboPooledDataSource pool = new ComboPooledDataSource();
    pool.setDriverClass(DRIVER_IMP);
    pool.setJdbcUrl(url);
    return pool;
  }

  public static void verifyCount(int expected, Connection db) throws Exception {
    verifyCount(expected, db, "SEQUENCES");
  }

  public static void verifyCount(int expected, Connection db, String flywayTableName) throws Exception {
    int count = 0;
    String sql = String.format("SELECT * FROM \"%s\"", flywayTableName);
    try (PreparedStatement s = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery()) {
      if (rs.last()) {
        count = rs.getRow();
      }
    }
    assertEquals(expected, count);
  }

  public static void logTables(Connection db) throws Exception {
    try (PreparedStatement s = db.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE='T'");
        ResultSet rs = s.executeQuery()) {
      while (rs.next()) {
        System.err.println(rs.getString(1));
      }
    }
  }

}
