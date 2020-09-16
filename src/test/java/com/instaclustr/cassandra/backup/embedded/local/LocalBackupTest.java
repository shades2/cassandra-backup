package com.instaclustr.cassandra.backup.embedded.local;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.Manifest;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.cassandra.backup.impl.Snapshots;
import com.instaclustr.cassandra.backup.impl.Snapshots.Snapshot;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.cassandra.backup.impl.backup.BackupModules.BackupModule;
import com.instaclustr.cassandra.backup.impl.backup.BackupModules.UploadingModule;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker.UploadSession;
import com.instaclustr.cassandra.backup.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.cassandra.backup.impl.backup.coordination.ClearSnapshotOperation;
import com.instaclustr.cassandra.backup.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.coordination.TakeSnapshotOperation;
import com.instaclustr.cassandra.backup.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.local.LocalFileBackuper;
import com.instaclustr.cassandra.backup.local.LocalFileModule;
import com.instaclustr.cassandra.backup.local.LocalFileRestorer;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.threading.Executors;
import com.instaclustr.threading.ExecutorsModule;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LocalBackupTest extends AbstractBackupTest {

    @Inject
    public Optional<OperationCoordinator<BackupOperationRequest>> operationCoordinator;

    @Inject
    public CassandraJMXService jmxService;

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new LocalFileModule());
            add(new ExecutorsModule());
            add(new UploadingModule());
            add(new BackupModule());
            add(new CassandraModule(new CassandraJMXConnectionInfo()));
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceBackupRestoreTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveBackupRestoreTest(importArguments());
    }

    @Test
    public void testHardlinksBackupAndRestore() throws Exception {
        liveBackupRestoreTest(hardlinkingArguments());
    }

    @Test
    public void testDownloadOfRemoteManifest() throws Exception {
        try {
            RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();

            FileUtils.createDirectory(Paths.get(target("backup1") + "/cluster/test-dc/1/manifests").toAbsolutePath());

            restoreOperationRequest.storageLocation = new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1");

            Files.write(Paths.get("target/backup1/cluster/test-dc/1/manifests/snapshot-name-" + UUID.randomUUID().toString()).toAbsolutePath(),
                        "hello".getBytes(),
                        StandardOpenOption.CREATE_NEW
            );

            LocalFileRestorer localFileRestorer = new LocalFileRestorer(restoreOperationRequest);

            final Path downloadedFile = localFileRestorer.downloadNodeFileToDir(Paths.get("/tmp"), Paths.get("manifests"), s -> s.contains("snapshot-name-"));

            assertTrue(Files.exists(downloadedFile));
        } finally {
            deleteDirectory(Paths.get(target("backup1")));
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    @Override
    protected String getStorageLocation() {
        return "file://" + target("backup1") + "/cluster/datacenter1/node1";
    }


    @Test
    public void testUploadTracker() throws Exception {

        final String snapshotName = UUID.randomUUID().toString();
        final String snapshotName2 = UUID.randomUUID().toString();

        final BackupOperationRequest backupOperationRequest = getBackupOperationRequestForTracker(snapshotName, "test,test2");
        final BackupOperationRequest backupOperationRequest2 = getBackupOperationRequestForTracker(snapshotName2, "test");

        UploadTracker uploadTracker = null;

        try {
            startDatabase();
            initialiseDatabase(null);

            final AtomicBoolean wait = new AtomicBoolean(true);

            uploadTracker = new UploadTracker(new Executors.FixedTasksExecutorSupplier().get(10),
                                              new Executors.FixedTasksExecutorSupplier().get(10)) {
                // override for testing purposes
                @Override
                public UploadUnit constructUploadUnit(final Backuper backuper,
                                                      final ManifestEntry manifestEntry,
                                                      final AtomicBoolean shouldCancel,
                                                      final String snapshotTag) {
                    return new TestingUploadUnit(wait, backuper, manifestEntry, shouldCancel, snapshotTag);
                }
            };

            final LocalFileBackuper backuper = new LocalFileBackuper(backupOperationRequest);

            new TakeSnapshotOperation(jmxService,
                                      new TakeSnapshotOperationRequest(backupOperationRequest.entities,
                                                                       backupOperationRequest.snapshotTag)).run();

            new TakeSnapshotOperation(jmxService,
                                      new TakeSnapshotOperationRequest(backupOperationRequest2.entities,
                                                                       backupOperationRequest2.snapshotTag)).run();

            final Snapshots snapshots = Snapshots.parse(cassandraDataDir);
            final Optional<Snapshot> snapshot = snapshots.get(backupOperationRequest.snapshotTag);
            final Optional<Snapshot> snapshot2 = snapshots.get(backupOperationRequest2.snapshotTag);

            assert snapshot.isPresent();
            assert snapshot2.isPresent();

            final BackupOperation backupOperation = new BackupOperation(operationCoordinator, backupOperationRequest);
            final BackupOperation backupOperation2 = new BackupOperation(operationCoordinator, backupOperationRequest2);

            final List<ManifestEntry> manifestEntries = Manifest.from(snapshot.get()).getManifestEntries();
            final List<ManifestEntry> manifestEntries2 = Manifest.from(snapshot2.get()).getManifestEntries();

            UploadSession session = uploadTracker.submit(backuper, backupOperation, manifestEntries, backupOperation.request.snapshotTag);

            final int submittedUnits1 = uploadTracker.submittedUnits.intValue();
            Assert.assertEquals(manifestEntries.size(), submittedUnits1);

            final UploadSession session2 = uploadTracker.submit(backuper, backupOperation2, manifestEntries2, backupOperation.request.snapshotTag);
            final int submittedUnits2 = uploadTracker.submittedUnits.intValue();

            // even we submitted the second session, it does not change the number of units because session2
            // wants to upload "test" but it is already going to be uploaded by session1
            // we have effectively submitted only what should be submitted, no duplicates
            // so it is as if "test" from session2 was not submitted at all

            Assert.assertEquals(submittedUnits1, submittedUnits2);
            Assert.assertEquals(manifestEntries.size(), uploadTracker.submittedUnits.intValue());

            // however we have submitted two sessions in total
            Assert.assertEquals(2, uploadTracker.submittedSessions.intValue());

            // lets upload it now
            wait.set(false);

            session.waitUntilConsideredFinished();
            session2.waitUntilConsideredFinished();

            Assert.assertTrue(session.isConsideredFinished());
            Assert.assertTrue(session.isSuccessful());
            Assert.assertTrue(session.getFailedUnits().isEmpty());

            Assert.assertEquals(uploadTracker.submittedUnits.intValue(), session.getUnits().size());

            Assert.assertTrue(session2.isConsideredFinished());
            Assert.assertTrue(session2.isSuccessful());
            Assert.assertTrue(session2.getFailedUnits().isEmpty());

            Assert.assertTrue(submittedUnits2 > session2.getUnits().size());

            for (final UploadUnit uploadUnit : session2.getUnits()) {
                Assert.assertTrue(session.getUnits().contains(uploadUnit));
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            new ClearSnapshotOperation(jmxService, new ClearSnapshotOperationRequest(backupOperationRequest.snapshotTag)).run();
            stopCassandra();
            uploadTracker.stopAsync();
            uploadTracker.awaitTerminated(1, TimeUnit.MINUTES);
            uploadTracker.stopAsync();
            uploadTracker.awaitTerminated(1, TimeUnit.MINUTES);
        }
    }

    private BackupOperationRequest getBackupOperationRequestForTracker(final String snapshotName,
                                                                       final String entities) throws Exception {
        return new BackupOperationRequest(
            "backup",
            new StorageLocationTypeConverter().convert(getStorageLocation()),
            null,
            null,
            null,
            null,
            null,
            cassandraDir.resolve("data"),
            DatabaseEntities.parse(entities),
            snapshotName,
            null,
            null,
            false,
            null,
            false,
            null, // timeout
            false,
            false
        );
    }

    private static class TestingUploadUnit extends UploadUnit {

        private final AtomicBoolean wait;

        public TestingUploadUnit(final AtomicBoolean wait,
                                 final Backuper backuper,
                                 final ManifestEntry manifestEntry,
                                 final AtomicBoolean shouldCancel,
                                 final String snapshotTag) {
            super(backuper, manifestEntry, shouldCancel, snapshotTag);
            this.wait = wait;
        }

        @Override
        public Void call() {
            while (wait.get()) {
                ;
            }

            return super.call();
        }
    }
}
