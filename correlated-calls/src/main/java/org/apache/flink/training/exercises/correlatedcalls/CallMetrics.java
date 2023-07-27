package org.apache.flink.training.exercises.correlatedcalls;

public class CallMetrics {
    private String customerId;
    private long millisecondsOfUse = 0L;
    private long failedCalls = 0L;
    private long connectedCalls = 0L;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getMillisecondsOfUse() {
        return millisecondsOfUse;
    }

    public void setMillisecondsOfUse(long millisecondsOfUse) {
        this.millisecondsOfUse = millisecondsOfUse;
    }

    public long getFailedCalls() {
        return failedCalls;
    }

    public void setFailedCalls(long failedCalls) {
        this.failedCalls = failedCalls;
    }

    public long getConnectedCalls() {
        return connectedCalls;
    }

    public void setConnectedCalls(long connectedCalls) {
        this.connectedCalls = connectedCalls;
    }
    
    public void addCdr(CorrelatedCdr cdr) {
        if (cdr.getSipResponseCode() == 200) {
            this.connectedCalls++;
        } else {
            this.failedCalls++;
        }
        this.millisecondsOfUse += cdr.getCustomerSbcCallDurationInMilliseconds();
    }

    @Override
    public String toString() {
        return String.format("Call Metrics {'customerId': %s, 'millisecondsOfUse': %d, 'connectedCalls': %d, 'failedCalls': %d)", 
        customerId, millisecondsOfUse, connectedCalls, failedCalls);
    }
}
