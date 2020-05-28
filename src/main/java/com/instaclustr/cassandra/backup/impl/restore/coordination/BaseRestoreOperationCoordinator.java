package com.instaclustr.cassandra.backup.impl.restore.coordination;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static java.lang.String.format;

import java.util.Map;

import com.instaclustr.cassandra.backup.guice.RestorerFactory;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhaseResultGatherer;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategyResolver;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.ResultGatherer;

public abstract class BaseRestoreOperationCoordinator extends OperationCoordinator<RestoreOperationRequest> {

    private final Map<String, RestorerFactory> restorerFactoryMap;
    private final RestorationStrategyResolver restorationStrategyResolver;

    public BaseRestoreOperationCoordinator(final Map<String, RestorerFactory> restorerFactoryMap,
                                           final RestorationStrategyResolver restorationStrategyResolver) {
        this.restorerFactoryMap = restorerFactoryMap;
        this.restorationStrategyResolver = restorationStrategyResolver;
    }

    @Override
    public ResultGatherer<RestoreOperationRequest> coordinate(final Operation<RestoreOperationRequest> operation) throws OperationCoordinatorException {

        final RestoreOperationRequest request = operation.request;

        if (request.restorationStrategyType == IMPORT || request.restorationStrategyType == HARDLINKS) {
            if (request.importing == null) {
                throw new IllegalStateException(format("you can not run %s strategy and have 'import' empty!",
                                                       request.restorationStrategyType));
            }

            if (request.restorationPhase == null) {
                throw new IllegalStateException(format("you can not run %s strategy and have 'restorationPhase' empty!",
                                                       request.restorationStrategyType));
            }
        }

        final RestorationPhaseResultGatherer gatherer = new RestorationPhaseResultGatherer();

        try (final Restorer restorer = restorerFactoryMap.get(request.storageLocation.storageProvider).createRestorer(request)) {

            final RestorationStrategy restorationStrategy = restorationStrategyResolver.resolve(request);

            restorationStrategy.restore(restorer, operation);

            gatherer.gather(operation, null);
        } catch (Exception ex) {
            gatherer.gather(operation, ex);
        }

        return gatherer;
    }
}
