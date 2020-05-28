package com.instaclustr.cassandra.backup.impl;

import static java.lang.String.format;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.MoreObjects;
import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.guice.StorageProviders;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;

public class StorageLocation {

    private static final Pattern filePattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)/(.*)");
    private static final Pattern cloudPattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)");

    public String rawLocation;
    public String storageProvider;
    public String bucket;
    public String clusterId;
    public String datacenterId;
    public String nodeId;
    public Path fileBackupDirectory;
    public boolean cloudLocation;

    public StorageLocation(final String rawLocation) {

        if (rawLocation.endsWith("/")) {
            this.rawLocation = rawLocation.substring(0, rawLocation.length() - 1);
        } else {
            this.rawLocation = rawLocation;
        }

        if (this.rawLocation.startsWith("file")) {
            initializeFileBackupLocation(this.rawLocation);
        } else {
            cloudLocation = true;
            initializeCloudBackupLocation(this.rawLocation);
        }
    }

    private void initializeFileBackupLocation(final String backupLocation) {
        final Matcher matcher = filePattern.matcher(backupLocation);

        if (!matcher.matches()) {
            return;
        }

        this.rawLocation = matcher.group();
        this.storageProvider = matcher.group(1);
        this.fileBackupDirectory = Paths.get(matcher.group(2));
        this.bucket = matcher.group(3);
        this.clusterId = matcher.group(4);
        this.datacenterId = matcher.group(5);
        this.nodeId = matcher.group(6);

        if (fileBackupDirectory.toString().isEmpty()) {
            fileBackupDirectory = fileBackupDirectory.toAbsolutePath();
        }
    }

    private void initializeCloudBackupLocation(final String backupLocation) {
        final Matcher matcher = cloudPattern.matcher(backupLocation);

        if (!matcher.matches()) {
            return;
        }

        this.rawLocation = matcher.group();
        this.storageProvider = matcher.group(1);
        this.bucket = matcher.group(2);
        this.clusterId = matcher.group(3);
        this.datacenterId = matcher.group(4);
        this.nodeId = matcher.group(5);
    }

    public void validate() throws IllegalStateException {
        if (cloudLocation) {
            if (rawLocation == null || storageProvider == null || bucket == null || clusterId == null || datacenterId == null || nodeId == null) {
                throw new IllegalStateException(format("Backup location %s is not in form protocol://bucketName/clusterId/datacenterid/nodeId",
                                                       rawLocation));
            }
        } else if (rawLocation == null || storageProvider == null || bucket == null || clusterId == null || datacenterId == null || nodeId == null || fileBackupDirectory == null) {
            throw new IllegalStateException(format("Backup location %s is not in form file:///some/backup/path/clusterId/datacenterId/nodeId",
                                                   rawLocation));
        }
    }

    public static StorageLocation updateDatacenter(final StorageLocation oldLocation, final String dc) {
        final String withoutNodeId = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/"));
        final String withoutDatacenter = withoutNodeId.substring(0, withoutNodeId.lastIndexOf("/"));
        return new StorageLocation(withoutDatacenter + "/" + dc + "/" + oldLocation.nodeId);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, String nodeId) {
        return new StorageLocation(oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/") + 1) + nodeId);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, UUID nodeId) {
        return StorageLocation.updateNodeId(oldLocation, nodeId.toString());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("rawLocation", rawLocation)
            .add("storageProvider", storageProvider)
            .add("bucket", bucket)
            .add("clusterId", clusterId)
            .add("datacenterId", datacenterId)
            .add("nodeId", nodeId)
            .add("fileBackupDirectory", fileBackupDirectory)
            .add("cloudLocation", cloudLocation)
            .toString();
    }

    @Target({TYPE, PARAMETER, FIELD})
    @Retention(RUNTIME)
    @Constraint(validatedBy = ValidStorageLocation.StorageLocationValidator.class)
    public @interface ValidStorageLocation {

        String message() default "{com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationValidator.message}";

        Class<?>[] groups() default {};

        Class<? extends Payload>[] payload() default {};

        class StorageLocationValidator implements ConstraintValidator<ValidStorageLocation, StorageLocation> {

            private final Set<String> storageProviders;

            @Inject
            public StorageLocationValidator(final @StorageProviders Set<String> storageProviders) {
                this.storageProviders = storageProviders;
            }

            @Override
            public boolean isValid(final StorageLocation value, final ConstraintValidatorContext context) {

                if (value == null) {
                    return true;
                }

                context.disableDefaultConstraintViolation();

                try {
                    value.validate();
                } catch (Exception ex) {
                    context.buildConstraintViolationWithTemplate(format("Invalid backup location: %s",
                                                                        ex.getLocalizedMessage())).addConstraintViolation();
                    return false;
                }

                if (!storageProviders.contains(value.storageProvider)) {
                    context.buildConstraintViolationWithTemplate(format("Available providers: %s",
                                                                        Arrays.toString(storageProviders.toArray()))).addConstraintViolation();

                    return false;
                }

                return true;
            }
        }
    }

    public static class StorageLocationTypeConverter implements ITypeConverter<StorageLocation> {

        @Override
        public StorageLocation convert(final String value) throws Exception {
            if (value == null) {
                return null;
            }

            try {
                return new StorageLocation(value);
            } catch (final Exception ex) {
                throw new CommandLine.TypeConversionException(format("Invalid value of StorageLocation '%s', reason: %s",
                                                                     value,
                                                                     ex.getLocalizedMessage()));
            }
        }
    }

    public static class StorageLocationSerializer extends StdSerializer<StorageLocation> {

        public StorageLocationSerializer() {
            super(StorageLocation.class);
        }

        protected StorageLocationSerializer(final Class<StorageLocation> t) {
            super(t);
        }

        @Override
        public void serialize(final StorageLocation value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            if (value != null) {
                gen.writeString(value.rawLocation);
            }
        }
    }

    public static class StorageLocationDeserializer extends StdDeserializer<StorageLocation> {

        public StorageLocationDeserializer() {
            super(StorageLocation.class);
        }

        @Override
        public StorageLocation deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            final String valueAsString = p.getValueAsString();

            if (valueAsString == null) {
                return null;
            }

            try {
                return new StorageLocation(valueAsString);
            } catch (final Exception ex) {
                throw new InvalidFormatException(p, "Invalid StorageLocation.", valueAsString, StorageLocation.class);
            }
        }
    }
}
