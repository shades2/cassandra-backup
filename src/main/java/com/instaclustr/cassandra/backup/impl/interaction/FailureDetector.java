package com.instaclustr.cassandra.backup.impl.interaction;

import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraObjectNames.V3;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.FailureDetectorMBean;

public class FailureDetector implements CassandraInteraction<Integer> {

    private final CassandraJMXService cassandraJMXService;

    public FailureDetector(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public Integer act() throws Exception {
        return cassandraJMXService.doWithMBean(new FunctionWithEx<FailureDetectorMBean, Integer>() {
            @Override
            public Integer apply(final FailureDetectorMBean detector) {
                return detector.getDownEndpointCount();
            }
        }, FailureDetectorMBean.class, V3.FAILURE_DETECTOR_MBEAN_NAME);
    }
}