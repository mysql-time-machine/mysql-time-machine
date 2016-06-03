package com.booking.replication.coordinator;

import com.booking.replication.checkpoints.SafeCheckPoint;

/**
 * Created by bosko on 5/30/16.
 */
public interface CoordinatorInterface {

    boolean onLeaderElection(Runnable callback) throws InterruptedException;

    void storeSafeCheckPoint(SafeCheckPoint safeCheckPoint) throws Exception;

    SafeCheckPoint getSafeCheckPoint();

    String serialize(SafeCheckPoint checkPoint) throws Exception;
}
