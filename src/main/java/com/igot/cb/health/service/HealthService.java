package com.igot.cb.health.service
;

import com.igot.cb.common.model.SBApiResponse;


public interface HealthService {

    SBApiResponse checkHealthStatus() throws Exception;

}
