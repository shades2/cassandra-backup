package com.instaclustr.cassandra.backup.impl;

import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.Adler32;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableUtils {

    private static final Logger logger = LoggerFactory.getLogger(SSTableUtils.class);

    // Ver. 2.0 = instaclustr-recovery_codes-jb-1-Data.db
    // Ver. 2.1 = lb-1-big-Data.db
    // Ver. 2.2 = lb-1-big-Data.db
    // Ver. 3.0 = mc-1-big-Data.db
    private static final Pattern SSTABLE_RE = Pattern.compile("((?:[a-zA-Z0-9][a-zA-Z0-9_-]+[a-zA-Z0-9][a-zA-Z0-9_-]+-)?[a-z]{2}-(\\d+)(?:-big)?)-.*");
    private static final ImmutableList<String> DIGESTS = ImmutableList.of("crc32", "adler32", "sha1");
    private static final int SSTABLE_PREFIX_IDX = 1;
    private static final int SSTABLE_GENERATION_IDX = 2;
    private static final Pattern CHECKSUM_RE = Pattern.compile("^([a-zA-Z0-9]+).*");

    public static String sstableHash(Path path) throws IOException {
        final Matcher matcher = SSTABLE_RE.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IllegalStateException("Can't compute SSTable hash for " + path + ": doesn't taste like sstable");
        }

        for (String digest : DIGESTS) {
            final Path digestPath = path.resolveSibling(matcher.group(SSTABLE_PREFIX_IDX) + "-Digest." + digest);
            if (!Files.exists(digestPath)) {
                continue;
            }

            final Matcher matcherChecksum = CHECKSUM_RE.matcher(new String(Files.readAllBytes(digestPath), StandardCharsets.UTF_8));
            if (matcherChecksum.matches()) {
                return matcher.group(SSTABLE_GENERATION_IDX) + "-" + matcherChecksum.group(1);
            }
        }

        // Ver. 2.0 doesn't create hash file, so do it ourselves
        try {
            final Path dataFilePath = path.resolveSibling(matcher.group(SSTABLE_PREFIX_IDX) + "-Data.db");
            logger.warn("No digest file found, generating checksum based on {}.", dataFilePath);
            return matcher.group(SSTABLE_GENERATION_IDX) + "-" + calculateChecksum(dataFilePath);
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't generate checksum for " + path.toString());
        }
    }

    public static String calculateChecksum(final Path filePath) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(filePath)) {

            int bytesStart;
            int bytesPerChecksum = 10 * 1024 * 1024;

            // Get last 10 megabytes of file to use for checksum
            if (fileChannel.size() >= bytesPerChecksum) {
                bytesStart = toIntExact(fileChannel.size()) - bytesPerChecksum;
            } else {
                bytesStart = 0;
                bytesPerChecksum = (int) fileChannel.size();
            }

            fileChannel.position(bytesStart);
            final ByteBuffer bytesToChecksum = ByteBuffer.allocate(bytesPerChecksum);
            int bytesRead = fileChannel.read(bytesToChecksum, bytesStart);

            assert (bytesRead == bytesPerChecksum);

            // Adler32 because it's faster than SHA / MD5 and Cassandra uses it - https://issues.apache.org/jira/browse/CASSANDRA-5862
            final Adler32 adler32 = new Adler32();
            adler32.update(bytesToChecksum.array());

            return String.valueOf(adler32.getValue());
        }
    }

    public static Stream<ManifestEntry> ssTableManifest(Path snapshotDirectory, Path tableBackupPath) throws IOException {
        return Files.list(snapshotDirectory)
            .flatMap(path -> {
                if (isCassandra22SecIndex(path)) {
                    return tryListFiles(path);
                }
                return Stream.of(path);
            })
            .filter(path -> SSTABLE_RE.matcher(path.getFileName().toString()).matches())
            .sorted()
            .map(localPath -> {
                try {
                    final String hash = sstableHash(localPath);
                    final Path manifestComponentFileName = snapshotDirectory.relativize(localPath);
                    final Path parent = manifestComponentFileName.getParent();
                    Path backupPath = tableBackupPath;
                    if (parent != null) {
                        backupPath = backupPath.resolve(parent);
                    }
                    backupPath = backupPath.resolve(hash).resolve(manifestComponentFileName.getFileName());
                    return new ManifestEntry(backupPath, localPath, ManifestEntry.Type.FILE);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
    }

    private static Stream<? extends Path> tryListFiles(Path path) {
        try {
            return Files.list(path);
        } catch (IOException e) {
            logger.error("Failed to retrieve the file(s)", e);
            return Stream.empty();
        }
    }

    /**
     * Checks whether or not the given table path leads to a secondary index folder (for Cassandra 2.2 +)
     */
    private static boolean isCassandra22SecIndex(final Path tablepath) {
        return tablepath.toFile().isDirectory() && tablepath.getFileName().toString().startsWith(".");
    }
}
