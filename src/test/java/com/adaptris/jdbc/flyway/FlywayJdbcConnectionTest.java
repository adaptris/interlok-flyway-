package com.adaptris.jdbc.flyway;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import com.adaptris.core.jdbc.DatabaseConnectionCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;


@SuppressWarnings("deprecation")
public class FlywayJdbcConnectionTest extends DatabaseConnectionCase<FlywayJdbcConnection> {

  public FlywayJdbcConnectionTest() {
    super();
  }

  @Test
  public void testGetFlywayTable(){
    DefaultFlywayMigrator connection = new DefaultFlywayMigrator();
    assertNull(connection.getFlywayTable());
    connection.setFlywayTable("table");
    assertEquals("table", connection.getFlywayTable());
}

  @Test
  public void testGetFlyway() throws Exception {
    FlywayJdbcConnection connection = new FlywayJdbcConnection();
    assertNull(connection.getFlyway());
    // Should return us the dumb functional interface
    assertNotNull(connection.migrator());
    // which means I can do this.
    connection.migrator().migrate(null);
    FlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(true).withFlywayLocations("classpath:migration/partial");
    connection.setFlyway(migrator);
    assertSame(migrator, connection.getFlyway());
    assertSame(migrator, connection.migrator());
  }

  @Test
  public void testConnectionWhenInitialisedFullMigration() throws Exception {
    FlywayJdbcConnection con = configure(createConnection(), initialiseFlywayDatabase());
    try {
      con.setFlyway(new DefaultFlywayMigrator().withFlywayLocations(Collections.singletonList("classpath:migration/full")));
      LifecycleHelper.init(con);
      con.connect();
      FlywayMigratorTest.verifyCount(1, FlywayMigratorTest.connection(con.getConnectUrl()));
      FlywayMigratorTest.verifyCount(1, FlywayMigratorTest.connection(con.getConnectUrl()), "flyway_schema_history");
    } finally {
      LifecycleHelper.stopAndClose(con);
    }
  }

  @Test
  public void testConnectionWithAlternateFlywayTable() throws Exception
  {
    String table = "alternative_flyway_schema_history";
    FlywayJdbcConnection con = configure(createConnection(), initialiseFlywayDatabase());
    try {
      con.setFlyway(
              new DefaultFlywayMigrator()
                      .withFlywayLocations(Collections.singletonList("classpath:migration/full"))
                      .withFlywayTable(table)
      );
      LifecycleHelper.init(con);
      con.connect();
      FlywayMigratorTest.verifyCount(1,FlywayMigratorTest.connection(con.getConnectUrl()));
      //ensure schema history table has been used instead of default
      FlywayMigratorTest.verifyCount(1, FlywayMigratorTest.connection(con.getConnectUrl()), table);
    } finally {
      LifecycleHelper.stopAndClose(con);
    }
  }

  @Override
  protected FlywayJdbcConnection createConnection() {
    return new FlywayJdbcConnection();
  }

  @Override
  protected FlywayJdbcConnection configure(FlywayJdbcConnection flywayJdbcConnection) throws Exception {
    String url = initialiseDatabase();
    return configure(flywayJdbcConnection, url);
  }

  private FlywayJdbcConnection configure(FlywayJdbcConnection flywayJdbcConnection, String url) throws Exception {
    flywayJdbcConnection.setConnectUrl(url);
    flywayJdbcConnection.setDriverImp(DRIVER_IMP);
    flywayJdbcConnection.setTestStatement(DEFAULT_TEST_STATEMENT);
    flywayJdbcConnection.setDebugMode(true);
    flywayJdbcConnection.setConnectionAttempts(1);
    flywayJdbcConnection.setConnectionRetryInterval(new TimeInterval(10L, TimeUnit.MILLISECONDS.name()));
    flywayJdbcConnection.setAlwaysValidateConnection(false);
    // The always-validate tests require a database, so we need to baseline
    return flywayJdbcConnection
        .withFlyway(new DefaultFlywayMigrator().withBaseline(true).withFlywayLocations("classpath:migration/partial"));
  }

  protected String initialiseFlywayDatabase() throws Exception {
    return "jdbc:derby:memory:" + nameGen.safeUUID() + ";create=true";
  }

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }
}