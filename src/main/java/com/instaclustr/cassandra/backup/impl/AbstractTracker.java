package com.instaclustr.cassandra.backup.impl;

import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.CANCELLED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.FAILED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.FINISHED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.IGNORED;
import static com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit.State.NOT_STARTED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.instaclustr.cassandra.backup.impl.AbstractTracker.Session;
import com.instaclustr.cassandra.backup.impl.AbstractTracker.Unit;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTracker<UNIT extends Unit, SESSION extends Session<UNIT>> extends AbstractIdleService {

    protected Logger logger = LoggerFactory.getLogger(AbstractTracker.class);

    protected final ListeningExecutorService executorService;
    protected final ListeningExecutorService finisherExecutorService;

    protected final List<UNIT> units = Collections.synchronizedList(new ArrayList<>());
    protected final Set<SESSION> sessions = Collections.synchronizedSet(new HashSet<>());

    protected final String trackerId = UUID.randomUUID().toString();

    public AbstractTracker(final ListeningExecutorService executorService,
                           final ListeningExecutorService finisherExecutorService) {
        this.executorService = executorService;
        this.finisherExecutorService = finisherExecutorService;
    }

    @Override
    protected void startUp() throws Exception {
        logger.info("Starting tracker ...");
    }

    @Override
    protected void shutDown() throws Exception {

        logger.info("Shutting down executor service ...");
        executorService.shutdown();

        while (true) {
            if (executorService.awaitTermination(1, MINUTES)) {
                break;
            }
        }

        logger.info("Executor service terminated.");

        logger.info("Shutting down finished executor service ...");

        finisherExecutorService.shutdown();

        while (true) {
            if (finisherExecutorService.awaitTermination(1, MINUTES)) {
                break;
            }
        }

        logger.info("Finisher executor service terminated.");
    }

    public int numberOfUnits() {
        return units.size();
    }

    public void removeSession(final SESSION session) {
        if (session != null) {
            sessions.remove(session);
        }
    }

    public Optional<SESSION> getSession(final String sessionId) {
        return getSession(UUID.fromString(sessionId));
    }

    public Optional<SESSION> getSession(final UUID sessionId) {
        return sessions.stream().filter(s -> s.getId().equals(sessionId)).findFirst();
    }

    public List<UNIT> getUnits() {
        return Collections.unmodifiableList(units);
    }

    public Set<SESSION> getSessions() {
        return Collections.unmodifiableSet(sessions);
    }

    public void cancelIfNecessary(final Session<? extends Unit> session) {
        if (session.isSuccessful()) {
            return;
        }

        // Non-failed unit is an unit which has not started yet
        // or it runs without an error so far.
        // Not-started unit is submitted to executor but it has not been executed yet,
        // the most probably because it waits until it fits into pool
        session.getNonFailedUnits().forEach(unit -> {
            if (unit.getState() == NOT_STARTED) {
                logger.info(format("Ignoring %s from processing because there was an errorneous unit in a session %s",
                                   unit.getManifestEntry().localFile,
                                   session.id));
                unit.setState(IGNORED);
            } else if (unit.getState() == Unit.State.RUNNING) {
                logger.info(format("Cancelling %s because there was an errorneous unit in a session %s",
                                   unit.getManifestEntry().localFile,
                                   session.id));
                unit.setState(CANCELLED);
                unit.shouldCancel.set(true);
            }
        });
    }

    public static abstract class Unit implements java.util.concurrent.Callable<Void> {

        @JsonIgnore
        protected String snapshotTag;
        protected final ManifestEntry manifestEntry;
        protected State state = NOT_STARTED;
        protected Throwable throwable = null;
        @JsonIgnore
        protected final AtomicBoolean shouldCancel;

        public Unit(final ManifestEntry manifestEntry,
                    final AtomicBoolean shouldCancel) {
            this.manifestEntry = manifestEntry;
            this.shouldCancel = shouldCancel;
        }

        public enum State {
            NOT_STARTED,
            RUNNING,
            FINISHED,
            FAILED,
            IGNORED,
            CANCELLED
        }

        public String getSnapshotTag() {
            return snapshotTag;
        }

        public void setSnapshotTag(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }

        public ManifestEntry getManifestEntry() {
            return manifestEntry;
        }

        public void setState(final State state) {
            this.state = state;
        }

        public State getState() {
            return state;
        }

        public boolean isErroneous() {
            return throwable != null;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        @JsonIgnore
        public AtomicBoolean getShouldCancel() {
            return shouldCancel;
        }
    }

    public static abstract class Session<U extends Unit> {

        private static final Logger logger = LoggerFactory.getLogger(Session.class);

        protected String snapshotTag;
        protected UUID id;
        protected final List<U> units = new ArrayList<>();

        public void setId(final UUID id) {
            this.id = id;
        }

        public UUID getId() {
            return id;
        }

        public String getSnapshotTag() {
            return snapshotTag;
        }

        public void setSnapshotTag(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }

        public List<U> getUnits() {
            return units;
        }

        public boolean isConsideredFinished() {
            return units.stream().anyMatch(unit -> unit.getState() == FAILED) ||
                units.stream().allMatch(unit -> unit.getState() == FINISHED);
        }

        public boolean isSuccessful() {
            return units.stream().noneMatch(unit -> unit.getState() == FAILED);
        }

        @JsonIgnore
        public List<U> getFailedUnits() {
            if (isSuccessful()) {
                return Collections.emptyList();
            }

            return units.stream().filter(unit -> unit.getState() == FAILED).collect(toList());
        }

        public List<U> getNonFailedUnits() {
            return units.stream().filter(unit -> unit.getState() != FAILED).collect(toList());
        }

        @JsonIgnore
        public void waitUntilConsideredFinished() {
            Awaitility.await().forever().pollInterval(5, TimeUnit.SECONDS).until(this::isConsideredFinished);
            logger.info(format("%sSession %s has finished %s",
                               snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                               id,
                               isSuccessful() ? "successfully" : "errorneously"));
        }
    }
}
