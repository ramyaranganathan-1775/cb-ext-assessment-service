package com.igot.cb.cache;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class RedisCacheMgr {

    private static int cache_ttl = 84600;

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private JedisPool jedisDataPopulationPool;

    @Autowired
    CbExtAssessmentServerProperties cbExtAssessmentServerProperties;

    private final Logger logger = LoggerFactory.getLogger(RedisCacheMgr.class);

    ObjectMapper objectMapper = new ObjectMapper();

    private static int questions_cache_ttl = 84600;

    @PostConstruct
    public void postConstruct() {
        this.questions_cache_ttl = cbExtAssessmentServerProperties.getRedisQuestionsReadTimeOut().intValue();
        if (!StringUtils.isEmpty(cbExtAssessmentServerProperties.getRedisTimeout())) {
            cache_ttl = Integer.parseInt(cbExtAssessmentServerProperties.getRedisTimeout());
        }
    }
    public void putCache(String key, Object object, int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("Error while putting cache data in Redis cache: ", e);
        }
    }
    public void putCache(String key, Object object) {
        putCache(key,object,cache_ttl);
    }
    public void putInQuestionCache(String key, Object object) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, questions_cache_ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("Error while putting Question data in Redis cache: ", e);
        }
    }
    public void putStringInCache(String key, String value,int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(Constants.REDIS_COMMON_KEY + key, value);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("Error while putting data in Redis cache: ", e);
        }
    }

    public void putStringInCache(String key, String value) {
        putStringInCache(key, value, cache_ttl);
    }

    public boolean deleteKeyByName(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
        	jedis.del(Constants.REDIS_COMMON_KEY + key);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error("Error while delete by key Name data in Redis cache: ", e);
            return false;
        }
    }

    public boolean deleteAllCBExtKey() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Set<String> keys = jedis.keys(keyPattern);
            for (String key : keys) {
            	jedis.del(key);
            }
            logger.info("All Keys starts with " + Constants.REDIS_COMMON_KEY + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error("Error while delete all data in Redis cache: ", e);
            return false;
        }
    }

    public String getCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("Error while getting data from Redis cache: ", e);
            return null;
        }
    }

    public List<String> mget(List<String> fields) {
        try (Jedis jedis = jedisPool.getResource()) {
        	String[] updatedKeys = new String[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
            	updatedKeys[i] = Constants.REDIS_COMMON_KEY + Constants.QUESTION_ID + fields.get(i);
            }
            return jedis.mget(updatedKeys);
        } catch (Exception e) {
            logger.error("Error while getting all data from Redis cache: ", e);
        }
        return null;
    }

    public Set<String> getAllKeyNames() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            return jedis.keys(keyPattern);
        } catch (Exception e) {
            logger.error("Error while getting all key Names from Redis cache: ", e);
            return Collections.emptySet();
        }
    }

    public List<Map<String, Object>> getAllKeysAndValues() {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Map<String, Object> res = new HashMap<>();
            Set<String> keys = jedis.keys(keyPattern);
            if (!keys.isEmpty()) {
                for (String key : keys) {
                    Object entries;
                    entries = jedis.get(key);
                    res.put(key, entries);
                }
                result.add(res);
            }
        } catch (Exception e) {
            logger.error("Error while getting all key and values from Redis cache: ", e);
            return Collections.emptyList();
        }
        return result;
    }
    
    public List<String> hget(String key, int index, String... fields) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            jedis.select(index);
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            logger.error("Error while getting index list from Redis cache: ", e);
            return null;
        }
    }

    public String getCache(String key, Integer index) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Error while getting Index from Redis cache: ", e);
            return null;
        }
    }

    public String getCacheFromDataRedish(String key, Integer index) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Failed to get key '{}' from Redis at index {}: {}", key, index, e.getMessage(), e);
            return null;
        }
    }

    public String getHashedCacheFromDataRedis(String key, Integer index, String field) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.hget(key,field);
        } catch (Exception e) {
            logger.error("Failed to fetch field '{}' from Redis hash '{}' at index {}: {}", field, key, index, e.getMessage(), e);
            return null;
        }
    }

    public String getContentFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Content from Redis cache: ", e);
            return null;
        }
    }

    public boolean keyExists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public boolean valueExists(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sismember(Constants.REDIS_COMMON_KEY + key, value);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public void putCacheAsStringArray(String key, String[] values, Integer ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            if(null == ttl)
                ttl = cache_ttl;
            jedis.sadd(Constants.REDIS_COMMON_KEY + key, values);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("An error occurred while saving data into Redis",e);
        }
    }

    public Set<String> getSetFromCacheAsCommaSeparated(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(10);
            return jedis.smembers(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Set from Redis cache: ", e);
            return null;
        }
    }
    public boolean isRedisHealthy() {
        try (Jedis jedis = jedisPool.getResource()) {
            return Constants.REDIS_PONG_RESPONSE.equalsIgnoreCase(jedis.ping());
        } catch (Exception e) {
            logger.error("Redis health check failed", e);
            return false;
        }
    }
}
