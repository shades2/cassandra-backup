package com.instaclustr.cassandra.backup.impl.restore;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.impl.AbstractTracker;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.restore.DownloadTracker.DownloadSession;
import com.instaclustr.cassandra.backup.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.cassandra.backup.impl.restore.RestoreModules.Downloading;
import com.instaclustr.cassandra.backup.impl.restore.RestoreModules.DownloadingFinisher;
import com.instaclustr.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadTracker extends AbstractTracker<DownloadUnit, DownloadSession> {

    private static final Logger logger = LoggerFactory.getLogger(DownloadTracker.class);

    @Inject
    public DownloadTracker(final @Downloading ListeningExecutorService executorService,
                           final @DownloadingFinisher ListeningExecutorService finisherExecutorService) {
        super(executorService, finisherExecutorService);
    }

    public DownloadUnit constructDownloadUnit(final Restorer restorer,
                                              final ManifestEntry manifestEntry,
                                              final AtomicBoolean shouldCancel,
                                              final String snapshotTag) {
        return new DownloadUnit(restorer, manifestEntry, shouldCancel, snapshotTag);
    }

    public DownloadSession submit(final Restorer restorer,
                                  final Operation<? extends BaseRestoreOperationRequest> operation,
                                  final Collection<ManifestEntry> entries,
                                  final String snapshotTag) {
        synchronized (units) {
            logger.info("Using downloading tracker {}", trackerId);

            final DownloadSession downloadSession = new DownloadSession();
            downloadSession.setSnapshotTag(snapshotTag);
            downloadSession.setId(operation.id);

            if (entries.isEmpty()) {
                logger.info("0 files to upload.");
                return downloadSession;
            }

            for (final ManifestEntry entry : entries) {

                DownloadUnit alreadySubmitted = null;

                final Iterator<DownloadUnit> it = units.iterator();

                while (it.hasNext()) {
                    DownloadUnit unit = it.next();

                    if (unit.getManifestEntry().objectKey.equals(entry.objectKey)) {
                        alreadySubmitted = unit;
                        break;
                    }
                }

                if (alreadySubmitted == null) {
                    final DownloadUnit downloadUnit = constructDownloadUnit(restorer, entry, operation.getShouldCancel(), snapshotTag);

                    units.add(downloadUnit);

                    final ListenableFuture<Void> downloadingFuture = executorService.submit(downloadUnit);
                    downloadingFuture.addListener(() -> units.remove(downloadUnit), finisherExecutorService);
                } else {
                    logger.info("Skipping manifest entry from downloading {} as it is already submitted.",
                                alreadySubmitted.getManifestEntry().objectKey);

                    downloadSession.getUnits().add(alreadySubmitted);
                }
            }

            sessions.add(downloadSession);

            return downloadSession;
        }
    }

    public static class DownloadSession extends AbstractTracker.Session<DownloadUnit> {

    }

    public static class DownloadUnit extends AbstractTracker.Unit {

        @JsonIgnore
        private final Restorer restorer;

        public DownloadUnit(final Restorer restorer,
                            final ManifestEntry manifestEntry,
                            final AtomicBoolean shouldCancel,
                            final String snapshotTag) {
            super(manifestEntry, shouldCancel);
            this.restorer = restorer;
            super.snapshotTag = snapshotTag;
        }

        @Override
        public Void call() {
            RemoteObjectReference remoteObjectReference = null;
            try {
                remoteObjectReference = restorer.objectKeyToNodeAwareRemoteReference(manifestEntry.objectKey);

                logger.info(String.format("Downloading file %s to %s.", remoteObjectReference.getObjectKey(), manifestEntry.localFile));

                Path localPath = manifestEntry.localFile;

                if (remoteObjectReference.canonicalPath.endsWith("-schema.cql")) {
                    localPath = manifestEntry.localFile.getParent().resolve("schema.cql");
                }

                restorer.downloadFile(localPath, remoteObjectReference);

                logger.info(String.format("Successfully downloaded file %s to %s.", remoteObjectReference.getObjectKey(), localPath));

                return null;
            } catch (final Throwable t) {
                if (remoteObjectReference != null) {
                    logger.error(String.format("Failed to download file %s.", remoteObjectReference.getObjectKey()), t);
                }
            }

            return null;
        }
    }
}
