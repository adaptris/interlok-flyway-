package com.adaptris.jdbc.flyway;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import com.adaptris.core.jdbc.DatabaseConnectionCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;

public class FlywayPluggableConnectionTest extends DatabaseConnectionCase<FlywayPluggableConnection> {

  public FlywayPluggableConnectionTest() {
    super();
  }

  @Test
  public void testConnectionWhenInitialisedFullMigration() throws Exception {
    FlywayPluggableConnection con = configure(createConnection(), initialiseFlywayDatabase())
        .withFlyway(new DefaultFlywayMigrator().withFlywayLocations(Collections.singletonList("classpath:migration/full")));
    try {
      LifecycleHelper.init(con);
      con.connect();
      FlywayMigratorTest.verifyCount(1, FlywayMigratorTest.connection(con.getConnectUrl()));
    } finally {
      LifecycleHelper.stopAndClose(con);
    }
  }

  @Test
  public void testGetFlyway() throws Exception {
    FlywayPluggableConnection connection = new FlywayPluggableConnection();
    assertNull(connection.getFlyway());
    // Should return us the dumb functional interface
    assertNotNull(connection.migrator());
    // which means I can do this.
    connection.migrator().migrate(null);
    FlywayMigrator migrator = new DefaultFlywayMigrator().withBaseline(false).withFlywayLocations("classpath:migration/full");
    connection.setFlyway(migrator);
    assertSame(migrator, connection.getFlyway());
    assertSame(migrator, connection.migrator());
  }

  @Override
  protected FlywayPluggableConnection createConnection() {
    return new FlywayPluggableConnection();
  }

  @Override
  protected FlywayPluggableConnection configure(FlywayPluggableConnection flywayJdbcConnection) throws Exception {
    String url = initialiseDatabase();
    return configure(flywayJdbcConnection, url);
  }

  private FlywayPluggableConnection configure(FlywayPluggableConnection flywayJdbcConnection, String url) throws Exception {
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