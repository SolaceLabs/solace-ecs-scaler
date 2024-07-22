package com.solace.scalers.aws_ecs.model.semp_v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SempMessageVpnStateResponse {

    @JsonProperty
    protected MessageVpnData data;

    @Data
    @NoArgsConstructor
    public static class MessageVpnData {
        @JsonProperty
        protected String state;
    }
}
