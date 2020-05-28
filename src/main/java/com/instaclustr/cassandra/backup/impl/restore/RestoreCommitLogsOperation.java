package com.instaclustr.cassandra.backup.impl.restore;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.cassandra.backup.guice.RestorerFactory;
import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreCommitLogsOperation extends Operation<RestoreCommitLogsOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RestoreCommitLogsOperation.class);

    private final static String CASSANDRA_COMMIT_LOGS = "commitlog";

    private final Map<String, RestorerFactory> restorerFactoryMap;

    private final Path commitlogsPath;

    @Inject
    public RestoreCommitLogsOperation(final Map<String, RestorerFactory> restorerFactoryMap,
                                      @Assisted final RestoreCommitLogsOperationRequest request) {
        super(request);

        this.restorerFactoryMap = restorerFactoryMap;
        commitlogsPath = request.cassandraDirectory.resolve(CASSANDRA_COMMIT_LOGS);
    }

    @Override
    protected void run0() throws Exception {

        final FileLock fileLock = new GlobalLock(request.lockFile).waitForLock();

        try (final Restorer restorer = restorerFactoryMap.get(request.storageLocation.storageProvider).createCommitLogRestorer(request)) {
            backupCurrentCommitLogs();
            downloadCommitLogs(restorer);
            updateCommitLogArchivingProperties();
            writeConfigOptions();

        } finally {
            fileLock.release();
        }
    }

    private void backupCurrentCommitLogs() throws Exception {
        final Set<Path> existingCommitlogsList = new HashSet<>();

        if (commitlogsPath.toFile().exists()) {
            try (Stream<Path> paths = Files.list(commitlogsPath)) {
                paths.filter(Files::isRegularFile).forEach(existingCommitlogsList::add);
            }
        }

        if (existingCommitlogsList.size() > 0) {
            final Path currentCommitlogsPath = commitlogsPath.getParent().resolve("commitlogs-" + System.currentTimeMillis());

            if (!currentCommitlogsPath.toFile().exists()) {
                Files.createDirectory(currentCommitlogsPath);
            }

            for (final Path file : existingCommitlogsList) {
                Files.move(file, currentCommitlogsPath.resolve(file.getFileName()));
            }
        }
    }

    private void downloadCommitLogs(final Restorer restorer) throws Exception {
        final RemoteObjectReference remoteObjectReference = restorer.objectKeyToRemoteReference(Paths.get("commitlog"));
        final Pattern commitlogPattern = Pattern.compile(".*(CommitLog-\\d+-\\d+\\.log)\\.(\\d+)");
        final HashSet<ManifestEntry> parsedCommitlogList = new HashSet<>();

        logger.info("Commencing processing of commit log listing");
        final AtomicReference<ManifestEntry> overhangingManifestEntry = new AtomicReference<>();
        final AtomicLong overhangingTimestamp = new AtomicLong(Long.MAX_VALUE);

        restorer.consumeFiles(remoteObjectReference, commitlogFile -> {

            final Matcher matcherCommitlog = commitlogPattern.matcher(commitlogFile.getObjectKey().toString());

            if (matcherCommitlog.matches()) {
                final long commitlogTimestamp = Long.parseLong(matcherCommitlog.group(2));

                if (commitlogTimestamp >= request.timestampStart && commitlogTimestamp <= request.timestampEnd) {
                    parsedCommitlogList.add(new ManifestEntry(commitlogFile.getObjectKey(),
                                                              request.commitlogDownloadDir.resolve(matcherCommitlog.group(1)),
                                                              ManifestEntry.Type.FILE,
                                                              0,
                                                              null));
                } else if (commitlogTimestamp > request.timestampEnd && commitlogTimestamp < overhangingTimestamp.get()) {
                    // Make sure we also catch the first commitlog that goes past the end of the timestamp
                    overhangingTimestamp.set(commitlogTimestamp);
                    overhangingManifestEntry.set(new ManifestEntry(commitlogFile.getObjectKey(),
                                                                   request.commitlogDownloadDir.resolve(matcherCommitlog.group(1)),
                                                                   ManifestEntry.Type.FILE,
                                                                   0,
                                                                   null));
                }
            }
        });

        if (overhangingManifestEntry.get() != null) {
            parsedCommitlogList.add(overhangingManifestEntry.get());
        }

        logger.info("Found {} commit logs to download", parsedCommitlogList.size());

        if (parsedCommitlogList.size() == 0) {
            return;
        }

        restorer.downloadFiles(parsedCommitlogList, new OperationProgressTracker(this, parsedCommitlogList.size()));
    }

    private void updateCommitLogArchivingProperties() {
        final Path commitlogArchivingPropertiesPath = request.cassandraConfigDirectory.resolve("commitlog_archiving.properties");
        Properties commitlogArchivingProperties = new Properties();

        if (commitlogArchivingPropertiesPath.toFile().exists()) {
            try (final BufferedReader reader = Files.newBufferedReader(commitlogArchivingPropertiesPath, StandardCharsets.UTF_8)) {
                commitlogArchivingProperties.load(reader);
            } catch (final IOException ex) {
                logger.warn("Failed to load file \"{}\".", commitlogArchivingPropertiesPath, ex);
            }
        }

        final SimpleDateFormat df = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        String timestamp = df.format(request.timestampEnd);

        commitlogArchivingProperties.setProperty("restore_command", "cp -f %from %to");
        commitlogArchivingProperties.setProperty("restore_directories", request.commitlogDownloadDir.toString());
        // Restore mutations created up to and including this timestamp in GMT.
        // Format: yyyy:MM:dd HH:mm:ss (2012:04:31 20:43:12)
        commitlogArchivingProperties.setProperty("restore_point_in_time", timestamp);

        try (final OutputStream output = new FileOutputStream(commitlogArchivingPropertiesPath.toFile())) {
            commitlogArchivingProperties.store(output, null);

            logger.info(format("file %s was updated. Please consult the content of this file before starting the node.",
                               commitlogArchivingPropertiesPath.toAbsolutePath().toString()));
        } catch (final IOException e) {
            logger.warn("Failed to write to file \"{}\".", commitlogArchivingPropertiesPath, e);
        }
    }

    /**
     * Add cassandra.replayList at the end of cassandra-env.sh. In case Cassandra container restarts,
     * this change will not be there anymore because configuration volume is ephemeral and gets
     * reconstructed every time from scratch.
     * <p>
     * However, when restoration tooling is run just against "vanilla" C* (without K8S), that added line will be there
     * "for ever".
     *
     * @throws IOException
     */
    private void writeConfigOptions() throws IOException {
        if (request.keyspaceTables.size() > 0) {
            final Path cassandraEnvSh = request.cassandraConfigDirectory.resolve("cassandra-env.sh");

            if (!cassandraEnvSh.toFile().exists()) {
                logger.error(String.format("%s was not updated to the -Dcassandra.replayList property because it does not exist.", cassandraEnvSh));
            } else {
                final String cassandraEnvStringBuilder = "JVM_OPTS=\"$JVM_OPTS -Dcassandra.replayList=" +
                    Joiner.on(",").withKeyValueSeparator(".").join(request.keyspaceTables.entries()) + "\"\n";

                Files.write(request.cassandraConfigDirectory.resolve("cassandra-env.sh"),
                            cassandraEnvStringBuilder.getBytes(),
                            StandardOpenOption.APPEND,
                            StandardOpenOption.CREATE);

                logger.info(String.format("%s was updated to contain the -Dcassandra.replayList property", cassandraEnvSh));
            }
        }
    }
}
