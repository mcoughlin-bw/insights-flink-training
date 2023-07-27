package org.apache.flink.training.exercises.correlatedcalls;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * This SourceFunction generates a data stream of CorrelatedCdr records.
 */
public class CorrelatedCdrGenerator implements SourceFunction<CorrelatedCdr> {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private final Random random = new Random();
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<CorrelatedCdr> ctx) throws Exception {
        List<Pair<String, Double>> customerIdPairs = new ArrayList<>();
        customerIdPairs.add(Pair.create("12345", 0.3));
        customerIdPairs.add(Pair.create("67890", 0.2));
        customerIdPairs.add(Pair.create("54321", 0.4));
        customerIdPairs.add(Pair.create("09876", 0.1));
        EnumeratedDistribution<String> customerIds = new EnumeratedDistribution<>(customerIdPairs);

        List<Pair<Integer, Double>> sipResponseCodePairs = new ArrayList<>();
        sipResponseCodePairs.add(Pair.create(200, 0.9));
        sipResponseCodePairs.add(Pair.create(487, 0.1));
        EnumeratedDistribution<Integer> sipResponseCodes = new EnumeratedDistribution<>(sipResponseCodePairs);

        while (running) {

            // generate a batch of CDRs
            for (int i = 1; i <= BATCH_SIZE; i++) {
                CorrelatedCdr cdr = new CorrelatedCdr();
                cdr.setCustomerId(customerIds.sample());
                cdr.setSipResponseCode(sipResponseCodes.sample());

                long randomEpoch = System.currentTimeMillis() - random.nextInt(10_000);
                cdr.setCustomerSbcDisconnectTimeInEpochMilliseconds(randomEpoch);
                if (cdr.getSipResponseCode() == 200) {
                    cdr.setCustomerSbcCallDurationInMilliseconds(random.nextInt(300_000));
                } else {
                    cdr.setCustomerSbcCallDurationInMilliseconds(0);
                }
                ctx.collect(cdr);
            }

            // throttle generation
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
