package com.instaclustr.cassandra.backup.azure;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.EnumSet;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.Backuper;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class AzureBackuper extends Backuper {

    private static final String DATE_TIME_METADATA_KEY = "LastFreshened";

    private final CloudBlobContainer blobContainer;

    private final CloudBlobClient cloudBlobClient;

    private final CloudStorageAccount cloudStorageAccount;

    @AssistedInject
    public AzureBackuper(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final BackupOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureBackuper(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final BackupCommitLogsOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = objectKey.toFile().toString();
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = resolveNodeAwareRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public FreshenResult freshenRemoteObject(final RemoteObjectReference object) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) object).blob;

        final Instant now = Instant.now();

        try {
            blob.getMetadata().put(DATE_TIME_METADATA_KEY, now.toString());
            blob.uploadMetadata();

            return FreshenResult.FRESHENED;

        } catch (final StorageException e) {
            if (e.getHttpStatusCode() != 404) {
                throw e;
            }

            return FreshenResult.UPLOAD_REQUIRED;
        }
    }

    @Override
    public void uploadFile(final long size,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        blob.upload(localFileStream, size);
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        blob.uploadText(text);
    }

    @Override
    public void cleanup() throws Exception {
        deleteStaleBlobs();
    }

    private void deleteStaleBlobs() throws Exception {
        final Date expiryDate = Date.from(ZonedDateTime.now().minusWeeks(1).toInstant());

        final CloudBlobDirectory directoryReference = blobContainer.getDirectoryReference(request.storageLocation.clusterId + "/" + request.storageLocation.datacenterId);

        for (final ListBlobItem blob : directoryReference.listBlobs(null, true, EnumSet.noneOf(BlobListingDetails.class), null, null)) {
            if (!(blob instanceof CloudBlob)) {
                continue;
            }

            final BlobProperties properties = ((CloudBlob) blob).getProperties();
            if (properties == null || properties.getLastModified() == null) {
                continue;
            }

            if (properties.getLastModified().before(expiryDate)) {
                ((CloudBlob) blob).delete();
            }
        }
    }
}
