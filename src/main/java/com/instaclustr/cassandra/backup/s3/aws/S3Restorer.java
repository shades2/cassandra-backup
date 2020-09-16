package com.instaclustr.cassandra.backup.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Restorer;
import com.instaclustr.cassandra.backup.s3.aws.S3Module.S3TransferManagerFactory;

public class S3Restorer extends BaseS3Restorer {

    @AssistedInject
    public S3Restorer(final S3TransferManagerFactory transferManagerFactory,
                      @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public S3Restorer(final S3TransferManagerFactory transferManagerFactory,
                      @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}
