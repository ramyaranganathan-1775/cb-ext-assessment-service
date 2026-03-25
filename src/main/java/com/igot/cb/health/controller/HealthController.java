package com.igot.cb.health.controller;

import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.health.service.HealthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;


@RestController
public class HealthController {

    @Autowired
    private HealthService healthService;

    @GetMapping("/health")
    public ResponseEntity<SBApiResponse> healthCheck() throws Exception {
        String requestId = UUID.randomUUID().toString();
        SBApiResponse response = healthService.checkHealthStatus(requestId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
