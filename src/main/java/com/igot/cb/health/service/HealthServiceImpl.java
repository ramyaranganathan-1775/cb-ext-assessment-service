package com.igot.cb.health.service;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.utils.CassandraOperation;
import com.igot.cb.common.model.SBApiResponse;

import com.igot.cb.common.util.Constants;
import com.igot.cb.common.util.ProjectUtil;
import jakarta.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HealthServiceImpl implements HealthService {


    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    RedisCacheMgr redisCacheService;

    @Autowired
    EntityManager entityManager;

    private Logger log = LoggerFactory.getLogger(getClass().getName());

    @Override
    public SBApiResponse checkHealthStatus() throws Exception {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_HEALTH_CHECK);
        try {
            response.put(Constants.HEALTHY, true);
            List<Map<String, Object>> healthResults = new ArrayList<>();
            response.put(Constants.CHECKS, healthResults);
            cassandraHealthStatus(response);
            redisHealthStatus(response);
            postgresHealthStatus(response);
        } catch (Exception e) {
            log.error("Failed to process health check. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public void cassandraHealthStatus(SBApiResponse response) throws Exception {
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.NAME, Constants.CASSANDRA_DB);
        Boolean res = true;
        List<Map<String, Object>> cassandraQueryResponse = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_SYSTEM_SETTINGS, null, null);

        if (cassandraQueryResponse.isEmpty()) {
            res = false;
            response.put(Constants.HEALTHY, res);
        }
        result.put(Constants.HEALTHY, res);
        ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);
    }

    private void redisHealthStatus(SBApiResponse response) {

        Map<String, Object> result = new HashMap<>();
        result.put(Constants.NAME, Constants.REDIS_CACHE);

        boolean isHealthy = redisCacheService.isRedisHealthy();

        result.put(Constants.HEALTHY, isHealthy);

        ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);

        if (!isHealthy) {
            response.put(Constants.HEALTHY, false);
        }
    }

    @Transactional(readOnly = true)
    public void postgresHealthStatus(SBApiResponse response) {
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.NAME, Constants.CASSANDRA_DB);
        Boolean res = true;
        try {
            entityManager.createNativeQuery("SELECT 1").getSingleResult();
            result.put(Constants.HEALTHY, res);
            ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);
        } catch (Exception e) {
            res = false;
            response.put(Constants.HEALTHY, res);
            result.put(Constants.HEALTHY, res);
            ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);
        }
    }



}

