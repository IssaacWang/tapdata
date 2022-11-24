package io.tapdata.connector.tdengine.subscribe;

import java.sql.Timestamp;

public class TDengineResultBean {
    private Timestamp ts;
    private int speed;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }
}
