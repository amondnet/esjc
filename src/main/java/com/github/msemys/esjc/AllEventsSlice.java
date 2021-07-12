package com.github.msemys.esjc;

import com.github.msemys.esjc.proto.EventStoreClientMessages;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toCollection;

/**
 * Result type of a single read operation to the Event Store, that retrieves event list from $all stream.
 */
public class AllEventsSlice {

    /**
     * The direction of read request.
     */
    public final ReadDirection readDirection;

    /**
     * The position where this slice was read from.
     */
    public final Position fromPosition;

    /**
     * The position where the next slice should be read from.
     */
    public final Position nextPosition;

    /**
     * The events read.
     */
    public final List<ResolvedEvent> events;

    /**
     *     A boolean representing whether or not this is the end of the $all stream.
     */
    private final boolean isEndOfStream;

    public AllEventsSlice(ReadDirection readDirection,
                          Position fromPosition,
                          Position nextPosition,
                          List<EventStoreClientMessages.ResolvedEvent> events) {
        this.readDirection = readDirection;
        this.fromPosition = fromPosition;
        this.nextPosition = nextPosition;
        this.events = (events == null) ? emptyList() : events.stream()
                .map(ResolvedEvent::new)
                .collect(toCollection(() -> new ArrayList<>(events.size())));
        this.isEndOfStream = events == null || events.size() == 0;
    }

    public AllEventsSlice(ReadDirection readDirection,
        Position fromPosition,
        Position nextPosition,
        List<EventStoreClientMessages.ResolvedEvent> events, boolean isEndOfStream) {
        this.readDirection = readDirection;
        this.fromPosition = fromPosition;
        this.nextPosition = nextPosition;
        this.events = (events == null) ? emptyList() : events.stream()
            .map(ResolvedEvent::new)
            .collect(toCollection(() -> new ArrayList<>(events.size())));
        this.isEndOfStream = isEndOfStream;
    }


    public boolean isEndOfStream () {
        return isEndOfStream;
    }
}
