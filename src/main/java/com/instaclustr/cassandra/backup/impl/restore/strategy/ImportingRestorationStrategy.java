package com.instaclustr.cassandra.backup.impl.restore.strategy;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static java.lang.String.format;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.CleaningPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.DownloadingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.ImportingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.InitPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.TruncatingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

/**
 * This strategy is supposed to be executed against a running Cassandra 4 node. If you run against Cassandra 3,
 * {@link HardlinkingRestorationStrategy} is preferred.
 *
 * <pre>
 * {@code
 * 1) restore the data in a temporary folder
 * 2) truncate data
 * 3) invoke nodetool import command via JMX (only present in Cassandra 4)
 * 4) clean temporary folder
 * }
 * </pre>
 */
public class ImportingRestorationStrategy extends AbstractRestorationStrategy {

    private final Provider<CassandraVersion> cassandraVersion;

    @Inject
    public ImportingRestorationStrategy(final CassandraJMXService cassandraJMXService,
                                        final Provider<CassandraVersion> cassandraVersion) {
        super(cassandraJMXService);
        this.cassandraVersion = cassandraVersion;
    }

    @Override
    public void isEligibleToRun() throws Exception {
        final CassandraVersion cassandraVersion = this.cassandraVersion.get();

        if (!CassandraVersion.isFour(this.cassandraVersion.get())) {
            throw new IllegalStateException("This type of restoration strategy can be used only against Cassandra 4. " +
                                                "You are running this restoration against " + cassandraVersion.toString());
        }
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return IMPORT;
    }

    @Override
    public RestorationPhase resolveRestorationPhase(final Operation<RestoreOperationRequest> operation, final Restorer restorer) {
        final RestorationPhaseType phaseType = operation.request.restorationPhase;

        if (phaseType == INIT) {
            return new InitPhase(operation);
        } else if (phaseType == DOWNLOAD) {
            return new DownloadingPhase(cassandraJMXService, operation, restorer);
        } else if (phaseType == TRUNCATE) {
            return new TruncatingPhase(cassandraJMXService, operation);
        } else if (phaseType == RestorationPhaseType.IMPORT) {
            return new ImportingPhase(cassandraJMXService, operation, cassandraVersion.get());
        } else if (phaseType == CLEANUP) {
            return new CleaningPhase(operation);
        }

        throw new IllegalStateException(format("Unable to resolve phase for phase type %s for %s.", phaseType, ImportingRestorationStrategy.class.getName()));
    }

}
