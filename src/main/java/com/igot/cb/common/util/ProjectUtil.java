package com.igot.cb.common.util;


import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.model.SunbirdApiRespParam;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;
import java.util.*;

public class ProjectUtil {

    public static SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(LocalDateTime.now().toString());
        return response;
    }

    public static void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    public static Map<String, Object> createDefaultMapResponse(String api, String err, String errMsg) {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.HEALTHY, Constants.TRUE);
        response.put(Constants.NAME, api);
        response.put(Constants.ERR, err != null ? err : "");
        response.put(Constants.ERROR_MESSAGE, errMsg != null ? errMsg : "");
        return response;
    }

}