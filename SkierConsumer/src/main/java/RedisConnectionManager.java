import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisConnectionManager {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);         // Allow up to 1000 connections
        poolConfig.setMaxIdle(200);          // Allow 200 idle connections
        poolConfig.setMinIdle(50);           // Ensure 50 always ready
        poolConfig.setBlockWhenExhausted(true); // Block when pool is exhausted
        poolConfig.setMaxWaitMillis(3000);   // Wait up to 3 seconds for a connection
        jedisPool = new JedisPool(poolConfig, Config.getRedisHost(), Config.getRedisPort(), 2000, Config.getRedisPassword());
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }

    public static void closePool() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
