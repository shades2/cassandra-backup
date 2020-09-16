package com.instaclustr.cassandra.backup.s3;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.google.common.io.CharStreams;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseS3Restorer extends Restorer {

    private static final Logger logger = LoggerFactory.getLogger(BaseS3Restorer.class);

    protected final AmazonS3 amazonS3;
    protected final TransferManager transferManager;

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final RestoreOperationRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
    }

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final RestoreCommitLogsOperationRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, objectKey.toFile().toString());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }

    @Override
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        final GetObjectRequest getObjectRequest = new GetObjectRequest(request.storageLocation.bucket, objectReference.canonicalPath);
        try (final InputStream is = transferManager.getAmazonS3Client().getObject(getObjectRequest).getObjectContent(); final InputStreamReader isr = new InputStreamReader(is)) {
            return CharStreams.toString(isr);
        }
    }

    @Override
    public void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        final GetObjectRequest getObjectRequest = new GetObjectRequest(request.storageLocation.bucket, objectReference.canonicalPath);

        Files.createDirectories(localPath.getParent());

        final Optional<AmazonClientException> exception = ofNullable(transferManager.download(getObjectRequest,
                                                                                              localPath.toFile(),
                                                                                              new DownloadProgressListener(objectReference)).waitForException());

        if (exception.isPresent()) {
            if (exception.get() instanceof AmazonS3Exception && ((AmazonS3Exception) exception.get()).getStatusCode() == 404) {
                logger.error("Remote object reference {} does not exist.", objectReference);
            }

            throw exception.get();
        }
    }

    @Override
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(remotePrefix.toString(), keyFilter);
        final String fileName = s3Object.getKey().split("/")[s3Object.getKey().split("/").length - 1];
        return downloadFileToString(objectKeyToRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.getKey().split("/")[s3Object.getKey().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.getKey().split("/")[s3Object.getKey().split("/").length - 1];
        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    private S3Object getBlobItemPath(final String remotePrefix, final Predicate<String> keyFilter) {
        ObjectListing objectListing = amazonS3.listObjects(request.storageLocation.bucket, remotePrefix);

        boolean hasMoreContent = true;

        final List<S3ObjectSummary> summaryList = new ArrayList<>();

        while (hasMoreContent) {
            objectListing.getObjectSummaries().stream()
                .filter(objectSummary -> !objectSummary.getKey().endsWith("/")) // no dirs
                .filter(file -> keyFilter.test(file.getKey()))
                .collect(toCollection(() -> summaryList));

            if (objectListing.isTruncated()) {
                objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                hasMoreContent = false;
            }
        }

        if (summaryList.size() != 1) {
            throw new IllegalStateException(String.format("There is not one key which satisfies key filter: %s", summaryList.toString()));
        }

        return amazonS3.getObject(request.storageLocation.bucket, summaryList.get(0).getKey());
    }

    private static class DownloadProgressListener implements S3ProgressListener {

        private final RemoteObjectReference objectReference;

        public DownloadProgressListener(final RemoteObjectReference objectReference) {
            this.objectReference = objectReference;
        }

        @Override
        public void onPersistableTransfer(final PersistableTransfer persistableTransfer) {
            // We don't resume downloads
        }

        @Override
        public void progressChanged(final ProgressEvent progressEvent) {
            if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                logger.debug("Successfully downloaded {}.", objectReference.canonicalPath);
            }
        }
    }


    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) {

        final Path bucketPath = Paths.get(request.storageLocation.clusterId).resolve(request.storageLocation.datacenterId).resolve(request.storageLocation.nodeId);

        ObjectListing objectListing = amazonS3.listObjects(request.storageLocation.bucket, prefix.canonicalPath);

        boolean hasMoreContent = true;

        while (hasMoreContent) {
            objectListing.getObjectSummaries().stream()
                .filter(objectSummary -> !objectSummary.getKey().endsWith("/"))
                .forEach(objectSummary -> consumer.accept(objectKeyToNodeAwareRemoteReference(bucketPath.relativize(Paths.get(objectSummary.getKey())))));

            if (objectListing.isTruncated()) {
                objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                hasMoreContent = false;
            }
        }
    }

    @Override
    public void cleanup() {
        transferManager.shutdownNow();
    }
}
