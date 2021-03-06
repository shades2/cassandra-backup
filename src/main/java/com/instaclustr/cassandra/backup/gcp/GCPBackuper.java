package com.instaclustr.cassandra.backup.gcp;

import static com.google.cloud.storage.Storage.PredefinedAcl.BUCKET_OWNER_FULL_CONTROL;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.io.ByteStreams;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class GCPBackuper extends Backuper {

    private final Storage storage;

    @AssistedInject
    public GCPBackuper(final GoogleStorageFactory storageFactory,
                       final ExecutorServiceSupplier executorServiceSupplier,
                       @Assisted final BackupOperationRequest backupOperationRequest) {
        super(backupOperationRequest, executorServiceSupplier);
        this.storage = storageFactory.build(backupOperationRequest);
    }

    @AssistedInject
    public GCPBackuper(final GoogleStorageFactory storageFactory,
                       final ExecutorServiceSupplier executorServiceSupplier,
                       @Assisted final BackupCommitLogsOperationRequest backupOperationRequest) {
        super(backupOperationRequest, executorServiceSupplier);
        this.storage = storageFactory.build(backupOperationRequest);
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        return new GCPRemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey), request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new GCPRemoteObjectReference(objectKey, objectKey.toString(), request.storageLocation.bucket);
    }

    @Override
    public FreshenResult freshenRemoteObject(final RemoteObjectReference object) {
        final BlobId blobId = ((GCPRemoteObjectReference) object).blobId;

        try {
            storage.copy(new Storage.CopyRequest.Builder()
                             .setSource(blobId)
                             .setTarget(BlobInfo.newBuilder(blobId).build(), Storage.BlobTargetOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL))
                             .build());

            return FreshenResult.FRESHENED;
        } catch (final StorageException e) {
            if (e.getCode() != 404) {
                throw e;
            }

            return FreshenResult.UPLOAD_REQUIRED;
        }
    }

    @Override
    public void uploadFile(final long size,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;

        try (final WriteChannel outputChannel = storage.writer(BlobInfo.newBuilder(blobId).build(), Storage.BlobWriteOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL));
            final ReadableByteChannel inputChannel = Channels.newChannel(localFileStream)) {
            ByteStreams.copy(inputChannel, outputChannel);
        }
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;
        storage.create(BlobInfo.newBuilder(blobId).build(), text.getBytes(), Storage.BlobTargetOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL));
    }

    @Override
    public void cleanup() throws Exception {
    }
}
