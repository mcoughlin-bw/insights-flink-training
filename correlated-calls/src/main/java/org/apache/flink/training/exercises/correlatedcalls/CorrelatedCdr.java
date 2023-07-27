package org.apache.flink.training.exercises.correlatedcalls;


public class CorrelatedCdr {
    private String customerId;
    private long customerSbcCallDurationInMilliseconds;
    private long customerSbcDisconnectTimeInEpochMilliseconds;
    private Integer sipResponseCode;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getCustomerSbcCallDurationInMilliseconds() {
        return customerSbcCallDurationInMilliseconds;
    }

    public void setCustomerSbcCallDurationInMilliseconds(long customerSbcCallDurationInMilliseconds) {
        this.customerSbcCallDurationInMilliseconds = customerSbcCallDurationInMilliseconds;
    }

    public long getCustomerSbcDisconnectTimeInEpochMilliseconds() {
        return customerSbcDisconnectTimeInEpochMilliseconds;
    }

    public void setCustomerSbcDisconnectTimeInEpochMilliseconds(long customerSbcDisconnectTimeInEpochMilliseconds) {
        this.customerSbcDisconnectTimeInEpochMilliseconds = customerSbcDisconnectTimeInEpochMilliseconds;
    }

    public Integer getSipResponseCode() {
        return sipResponseCode;
    }

    public void setSipResponseCode(Integer sipResponseCode) {
        this.sipResponseCode = sipResponseCode;
    }
}
