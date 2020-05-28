package com.instaclustr.cassandra.backup.impl.backup;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities.DatabaseEntitiesConverter;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities.DatabaseEntitiesDeserializer;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities.DatabaseEntitiesSerializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import picocli.CommandLine.Option;

@ValidBackupOperationRequest
public class BackupOperationRequest extends BaseBackupOperationRequest {

    @Option(names = {"-s", "--st", "--snapshot-tag"},
        description = "Snapshot tag name. Default is equiv. to 'autosnap-`date +s`'")
    @JsonProperty("snapshotTag")
    public String snapshotTag = format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis()));

    @Option(names = "--entities",
        description = "entities to backup, if not specified, all keyspaces will be backed up, form 'ks1,ks2,ks2' or 'ks1.cf1,ks2.cf2'",
        converter = DatabaseEntitiesConverter.class)
    @JsonProperty("entities")
    @JsonSerialize(using = DatabaseEntitiesSerializer.class)
    @JsonDeserialize(using = DatabaseEntitiesDeserializer.class)
    public DatabaseEntities entities;

    @Option(names = "--datacenter",
        description = "Name of datacenter against which restore will be done. It means that nodes in a different DC will not receive backup requests. "
            + "This is valid only in case globalRequest is true. Use with caution because when truncating a table as part of ")
    @JsonProperty("dc")
    public String dc;

    @JsonProperty("globalRequest")
    public boolean globalRequest;

    public BackupOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public BackupOperationRequest(@JsonProperty("type") final String type,
                                  @JsonProperty("storageLocation") final StorageLocation storageLocation,
                                  @JsonProperty("duration") final Time duration,
                                  @JsonProperty("bandwidth") final DataRate bandwidth,
                                  @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                  @JsonProperty("lockFile") final Path lockFile,
                                  @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                                  @JsonProperty("entities")
                                  @JsonSerialize(using = DatabaseEntitiesSerializer.class)
                                  @JsonDeserialize(using = DatabaseEntitiesDeserializer.class) final DatabaseEntities entities,
                                  @JsonProperty("snapshotTag") final String snapshotTag,
                                  @JsonProperty("k8sNamespace") final String k8sNamespace,
                                  @JsonProperty("k8sBackupSecretName") final String k8sBackupSecretName,
                                  @JsonProperty("globalRequest") final boolean globalRequest,
                                  @JsonProperty("dc") final String dc) {
        super(storageLocation, duration, bandwidth, concurrentConnections, cassandraDirectory, lockFile, k8sNamespace, k8sBackupSecretName);
        this.entities = entities == null ? DatabaseEntities.empty() : entities;
        this.snapshotTag = snapshotTag == null ? format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis())) : snapshotTag;
        this.globalRequest = globalRequest;
        this.type = type;
        this.dc = dc;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("lockFile", lockFile)
            .add("cassandraDirectory", cassandraDirectory)
            .add("entities", entities)
            .add("snapshotTag", snapshotTag)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sBackupSecretName)
            .add("globalRequest", globalRequest)
            .add("dc", dc)
            .toString();
    }
}
