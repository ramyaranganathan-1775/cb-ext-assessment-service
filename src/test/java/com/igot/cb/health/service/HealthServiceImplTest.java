package com.igot.cb.health.service;


import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.utils.CassandraOperation;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.Constants;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HealthServiceImplTest {

    @Mock CassandraOperation cassandraOperation;
    @Mock RedisCacheMgr redisCacheService;
    @Mock Query query;
    private final String REQUEST_ID = "test-request--123";
    @InjectMocks HealthServiceImpl service;
    @Mock
    SBApiResponse response;

    @Mock
    AdminClient adminClient;

    @Mock
    DescribeClusterResult describeClusterResult;

    @Mock
    KafkaFuture<Collection<Node>> kafkaFutureNodes;

    // 🔹 Common mocks
    void mockAllHealthy() throws Exception {

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any()))
                .thenReturn(List.of(Map.of("k", "v")));

        when(redisCacheService.isRedisHealthy()).thenReturn(true);

    }

    // ✅ SUCCESS CASE
    @Test
    void testHealthCheckSuccess() throws Exception {

        mockAllHealthy();

        response = service.checkHealthStatus(REQUEST_ID);

        assertNotNull(response);


        Map<String, Object> result =
                (Map<String, Object>) response.get(Constants.RESPONSE);

        assertNotNull(response);
        assertEquals(Constants.ALL_HEALTH_CHECK, result.get(Constants.NAME));

        List<Map<String, Object>> checks =
                (List<Map<String, Object>>) result.get(Constants.CHECKS);

        assertEquals(3, checks.size());
    }

    // ❌ FAILURE CASE (Redis down)
    @Test
    void testRedisFailure() throws Exception {

        mockAllHealthy();
        when(redisCacheService.isRedisHealthy()).thenReturn(false);

        response = service.checkHealthStatus(REQUEST_ID);

        assertFalse(Boolean.TRUE.equals(response.get(Constants.HEALTHY)));
    }

  /*  // ❌ FAILURE CASE (Postgres down)
    @Test
    void testPostgresFailure() throws Exception {

        mockAllHealthy();

        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.getSingleResult()).thenThrow(new RuntimeException("DB error"));

        SBApiResponse response = service.checkHealthStatus(REQUEST_ID);

        assertFalse(Boolean.TRUE.equals(response.get(Constants.HEALTHY)));
    }
*/
    // 💥 EXCEPTION CASE
    @Test
    void testExceptionHandling() throws Exception {

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB failure"));

        SBApiResponse response = service.checkHealthStatus("req-ex");

        assertNotNull(response);

        // ✅ overall unhealthy
        assertFalse(Boolean.TRUE.equals(response.get(Constants.HEALTHY)));

        // ✅ exception handled
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());

    }


    @Test
    void shouldReturnTrue_whenKafkaUp() throws Exception{
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(kafkaFutureNodes);
        when(kafkaFutureNodes.get(3, TimeUnit.SECONDS)).thenReturn(Collections.emptyList());

        List<Map<String, Object>> responseList = new ArrayList<>();

        SBApiResponse response = service.checkHealthStatus("req-empty");

        assertNotNull(response);
        Map<String, Object> result = (Map<String, Object>) response.getResult().get(Constants.RESPONSE);
        List<Map<String, Object>> checks = (List<Map<String, Object>>) result.get(Constants.CHECKS);

        assertNotNull(checks);

        checks.stream().filter(c -> Constants.KAFKA_SERVICE.equals(c.get(Constants.NAME)))
                .findFirst()
                .ifPresent(kafkaCheck -> {
                    assertEquals(Constants.TRUE, kafkaCheck.get(Constants.HEALTHY));
                });
    }

    @Test
    void shouldReturnTrue_whenKafkaDown() throws Exception{
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(kafkaFutureNodes);
        when(kafkaFutureNodes.get(3,TimeUnit.SECONDS)).thenThrow(new RuntimeException("Kafka connection failed"));

        List<Map<String, Object>> responseList = new ArrayList<>();

        SBApiResponse response = service.checkHealthStatus("req-empty");

        assertNotNull(response);
        Map<String, Object> result = (Map<String, Object>) response.getResult().get(Constants.RESPONSE);
        List<Map<String, Object>> checks = (List<Map<String, Object>>) result.get(Constants.CHECKS);

        assertNotNull(checks);

        checks.stream().filter(c -> Constants.KAFKA_SERVICE.equals(c.get(Constants.NAME)))
                .findFirst()
                .ifPresent(kafkaCheck -> {
                    assertEquals(500, kafkaCheck.get(Constants.ERR));
                });
    }

}



