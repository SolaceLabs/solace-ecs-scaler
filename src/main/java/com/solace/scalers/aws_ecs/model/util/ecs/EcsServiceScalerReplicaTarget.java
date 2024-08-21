package com.solace.scalers.aws_ecs.model.util.ecs;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EcsServiceScalerReplicaTarget {
    boolean scaleOutDecision = false;
    boolean scaleInDecision = false;
    Integer replicaTarget;

}
