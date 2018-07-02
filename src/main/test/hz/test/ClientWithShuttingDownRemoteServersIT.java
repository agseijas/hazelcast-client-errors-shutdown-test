package hz.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spring.cache.HazelcastCache;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.instance.HazelcastInstanceFactory.newHazelcastInstance;
import static java.lang.Thread.sleep;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = ClientWithShuttingDownRemoteServersIT.Config.class)
public class ClientWithShuttingDownRemoteServersIT
{
  private static final String CACHE_NAME = "myRemoteCache";

  private static final int INVOCATIONS = 3;

  private static final long A_MEGABYTE = 1_048_576;

  private static final long LARGE_CACHE_HEAP = 1_573_741_824;

  private static final int MASTER_REMOTE_PORT = 11111;

  private static final int SLAVE_REMOTE_PORT = 11112;

  private static final AtomicLong errorsOnPutDuringShutdown = new AtomicLong();
  private static final AtomicLong errorsOnGetDuringShutdown = new AtomicLong();

  private static final AtomicBoolean shutdownProcessTerminated = new AtomicBoolean(false);

  private String aRandomPreviouslyInsertedKey = "";

  @Autowired
  public Cache myRemoteCache;
  @Autowired
  HazelcastInstance remoteOtherServerHazelcastInstance;

  private ExecutorService executorService = newFixedThreadPool(INVOCATIONS + 1);

  /**
   * This test creates two member Hz instances that are in clustered (a master and a slave) mode.
   * It then connects a client Hz instance, once ready it launches up to $INVOCATIONS concurrent processes
   * to perform puts and gets continuously on the cache (through a SpringCache interface) with infinite
   * operation retries while capturing and printing any client cache error.
   * It also runs another process that polls the cache size continuously so that when it reaches $LARGE_CACHE_HEAP
   * it sends the shutdown command to the slave instance.
   * Once the shutdown is finished, sends the stop order to all concurrent processes so that the test can finish
   * after asserting on the number of client cache errors reported while on shutdown of the slave instance.
   * The effect is worsened by larger cache sizes (the migrations taking longer) and decreasing client timeouts
   *
   */
  @Test
  public void clientsShouldNotFailOnGetAndPutOperations_OnMemberNodeShutdowns() throws InterruptedException
  {
    //should have a client cache connected to two clustered instances up and running with myRemoteCache IMap
    assertThat(myRemoteCache, is(not(nullValue())));


    info("running up to " + INVOCATIONS +" processes to insert random strings into the IMap from a IMap client");
    concurrentlyRunAllProcesses(performingGetAndIncreasingPutOperationsOnTheCache());

    assertThat("should not have errors getting from the cache after shutdown", errorsOnGetDuringShutdown.get(), is(0L));
    assertThat("should not have errors putting from the cache after shutdown", errorsOnPutDuringShutdown.get(), is(0L));
  }

  private List<Callable<String>> performingGetAndIncreasingPutOperationsOnTheCache()
  {
    List<Callable<String>> withShutdownAdded = range(0, INVOCATIONS).mapToObj(this::putInfinitelyUntilItBreaks)
                                                                    .collect(toList());

    withShutdownAdded.add(processToShutdownAfterCacheSizeLimitIsReached());

    return withShutdownAdded;
  }

  private Callable<String> processToShutdownAfterCacheSizeLimitIsReached()
  {
    return () -> {
      long mapSize;
      IMap<Object, Object> myRemoteCache = remoteOtherServerHazelcastInstance.getMap(CACHE_NAME);

      do
      {
        sleep(2000);
        mapSize = myRemoteCache.getLocalMapStats().getHeapCost();
        info("filling cache... current size: " + mapSize / A_MEGABYTE + " MBytes");
      }
      while(mapSize <= LARGE_CACHE_HEAP);

      shutdown(remoteOtherServerHazelcastInstance);

      info("let the cache operations run for some seconds before finishing the test after shutting down the node");
      sleep(5000); //
      shutdownProcessTerminated.set(true);

      return "shutdown!!";
    };
  }

  private void shutdown(HazelcastInstance instance)
  {
    errorsOnPutDuringShutdown.set(0);
    errorsOnGetDuringShutdown.set(0);
    //remoteOtherServerHazelcastInstance.getCluster().changeClusterState(ClusterState.NO_MIGRATION, TransactionOptions.getDefault());
    info("shutting down the remote instance");
    instance.shutdown();
    info("errors after shutdown: " + errorsOnPutDuringShutdown);
  }

  private Callable<String> putInfinitelyUntilItBreaks(int ignored)
  {
    return () -> {
      long i = 0;
      do
      {
        performGetAndReportException();
        performPutAndReport(i, randomString());
        i++;
      }
      while (!shutdownProcessTerminated.get());
      return "";
    };
  }

  private void performPutAndReport(long i, String uuid)
  {

    try
    {
      myRemoteCache.put(uuid, fatObject(i, uuid));
      aRandomPreviouslyInsertedKey = uuid;
    }
    catch (Throwable hzError) {
      errorsOnPutDuringShutdown.getAndIncrement();
      error("Error on put Hz: ", hzError);
    }
  }

  private void performGetAndReportException()
  {
    try
    {
      myRemoteCache.get(aRandomPreviouslyInsertedKey);
    }
    catch (Throwable e)
    {
      errorsOnGetDuringShutdown.getAndIncrement();
      error("Error on Hz: ", e);
    }
  }

  private String fatObject(long i, String uuid)
  {
    Stream<String> objectStream = LongStream.range(0, i + 1).mapToObj(j -> uuid);
    return objectStream.collect(joining());
  }

  private void concurrentlyRunAllProcesses(List<Callable<String>> tasks) throws InterruptedException
  {
    executorService.invokeAll(tasks);
  }

  public static class Config
  {
    @Bean
    @DependsOn({"remoteServerHazelcastInstance", "remoteOtherServerHazelcastInstance"})
    public Cache myRemoteCache()
    {
      HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(createClientConfiguration());
      return new HazelcastCache(hazelcastInstance.getMap(CACHE_NAME));
    }

    private ClientConfig createClientConfiguration()
    {
      ClientConfig clientConfig = new ClientConfig();

      clientConfig.setInstanceName("test-hz-client");

      clientConfig.setProperty("hazelcast.client.request.retry.count", "1");
      clientConfig.setProperty("hazelcast.diagnostics.enabled", "true");
      clientConfig.setProperty("hazelcast.diagnostics.metric.level", "info");

      clientConfig.setProperty("hazelcast.map.invalidation.batch.enabled", "false");

      clientConfig.setProperty("hazelcast.client.invocation.timeout.seconds", "1");
      clientConfig.setProperty("hazelcast.client.heartbeat.interval","200");
      clientConfig.setProperty("hazelcast.client.heartbeat.timeout", "2000");

      ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
      networkConfig.addAddress("localhost:" + MASTER_REMOTE_PORT);
      networkConfig.getSocketOptions().setKeepAlive(true);
      info("setting retries on operations so that it fails after a timeout");
      networkConfig.setRedoOperation(true);
      networkConfig.setConnectionTimeout(1100);
      networkConfig.setConnectionAttemptPeriod(1500);
      networkConfig.setConnectionAttemptLimit(0);
      networkConfig.setSmartRouting(true);

      return clientConfig;
    }

    @Bean(destroyMethod = "shutdown")
    public HazelcastInstance remoteServerHazelcastInstance()
    {
      com.hazelcast.config.Config config = clusteredLocalRemoteServersConfig(MASTER_REMOTE_PORT);
      config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("localhost:" + SLAVE_REMOTE_PORT);

      addCacheConfig(config);

      return newHazelcastInstance(config);
    }

    @Bean(destroyMethod = "shutdown")
    @DependsOn({"remoteServerHazelcastInstance"})
    public HazelcastInstance remoteOtherServerHazelcastInstance()
    {
      com.hazelcast.config.Config config = clusteredLocalRemoteServersConfig(SLAVE_REMOTE_PORT);
      config.getNetworkConfig().getJoin().getTcpIpConfig().setRequiredMember("localhost:" + MASTER_REMOTE_PORT);

      info("######################################################################");
      info("#############  Instance to be shutdown (port): " + SLAVE_REMOTE_PORT + " #######################");
      info("######################################################################");

      addCacheConfig(config);

      return newHazelcastInstance(config);
    }

    private com.hazelcast.config.Config clusteredLocalRemoteServersConfig(int thisPort)
    {
      com.hazelcast.config.Config config = new com.hazelcast.config.Config();

      config.setProperty("hazelcast.discovery.enabled", "true");

      JoinConfig joinConfig = new JoinConfig();

      config.getNetworkConfig()
            .setPort(thisPort)
            .setJoin(joinConfig
              .setMulticastConfig(new MulticastConfig().setEnabled(false))
              .setTcpIpConfig(new TcpIpConfig()
                .setEnabled(true)));

      MapConfig mapConfig = config.getMapConfig(CACHE_NAME);
      mapConfig.setBackupCount(0);
      mapConfig.setAsyncBackupCount(1);

      config.setInstanceName(config.getInstanceName() + thisPort);

      return config;
    }

    private void addCacheConfig(com.hazelcast.config.Config config)
    {
      final MapConfig mapConfig = new MapConfig();
      mapConfig.setName(CACHE_NAME);
      config.addMapConfig(mapConfig);
    }
  }

  private static String randomString()
  {
    return randomUUID().toString();
  }

  private static void info(String s)
  {
    System.out.println(s);
  }
  private static void error(String s, Throwable hzError)
  {
    System.err.println(s);
    hzError.printStackTrace();
  }
}
