package com.github.mongobee.dao;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.testcontainers.containers.MongoDBContainer;

/**
 * @author colsson11
 * @since 13.01.15
 */
public class LockDaoTest {

  @ClassRule
  public static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0.16");

  private static final String TEST_SERVER = "testServer";
  private static final String DB_NAME = "mongobeetest";
  private static final String LOCK_COLLECTION_NAME = "mongobeelock";

  private static MongoClient testMongoClient;

  @BeforeClass
  public static void setUp() {
    String connectionString = mongoDBContainer.getReplicaSetUrl(DB_NAME);
    testMongoClient = new MongoClient(new MongoClientURI(connectionString));
  }

  @AfterClass
  public static void tearDown() {
    if (testMongoClient != null) {
      testMongoClient.close();
    }
  }

  private static MongoDatabase createTestDatabase() {
    return testMongoClient.getDatabase(DB_NAME);
  }

  @Test
  public void shouldGetLockWhenNotPreviouslyHeld() throws Exception {

    // given
    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.intitializeLock(db);

    // when
    boolean hasLock = dao.acquireLock(db);

    // then
    assertTrue(hasLock);

  }

  @Test
  public void shouldNotGetLockWhenPreviouslyHeld() throws Exception {

    // given
    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.intitializeLock(db);

    // when
    dao.acquireLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertFalse(hasLock);

  }

  @Test
  public void shouldGetLockWhenPreviouslyHeldAndReleased() throws Exception {

    // given
    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.intitializeLock(db);

    // when
    dao.acquireLock(db);
    dao.releaseLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertTrue(hasLock);

  }

  @Test
  public void releaseLockShouldBeIdempotent() {
    // given
    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);


    dao.intitializeLock(db);

    // when
    dao.releaseLock(db);
    dao.releaseLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertTrue(hasLock);

  }

  @Test
  public void whenLockNotHeldCheckReturnsFalse() {

    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.intitializeLock(db);

    assertFalse(dao.isLockHeld(db));

  }

  @Test
  public void whenLockHeldCheckReturnsTrue() {

    MongoDatabase db = createTestDatabase();
    db.drop(); // Clean up any previous test data
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.intitializeLock(db);

    dao.acquireLock(db);

    assertTrue(dao.isLockHeld(db));

  }

}
