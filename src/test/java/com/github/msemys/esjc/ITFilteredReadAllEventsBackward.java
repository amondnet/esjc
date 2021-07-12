package com.github.msemys.esjc;

import static com.github.msemys.esjc.matcher.RecordedEventListMatcher.containsInOrder;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class ITFilteredReadAllEventsBackward extends AbstractEventStoreTest {

    private List<EventData> testEvents;

    public ITFilteredReadAllEventsBackward(EventStore eventstore) {
        super(eventstore);
    }

    public List<EventData> getEvenIndexedEvent(List<EventData> events) {
        List<EventData> evenIndexedEvents = IntStream
            .range(0, events.size())
            .filter(i -> i % 2 == 0)
            .mapToObj(i -> events.get(i))
            .collect(Collectors.toList());

        return evenIndexedEvents;
    }

    public List<EventData> getOddIndexedEvent(List<EventData> events) {
        List<EventData> oddIndexedEvents = IntStream
            .range(0, events.size())
            .filter(i -> i % 2 == 1)
            .mapToObj(i -> events.get(i))
            .collect(Collectors.toList());

        return oddIndexedEvents;
    }

    @Before
    public void when() {
        testEvents = newTestEvents(10, "AEvent");
        testEvents.addAll(newTestEvents(10, "BEvent"));

        eventstore
            .appendToStream("stream-a", ExpectedVersion.NO_STREAM, getEvenIndexedEvent(testEvents))
            .join();
        eventstore
            .appendToStream("stream-b", ExpectedVersion.NO_STREAM, getOddIndexedEvent(testEvents))
            .join();

    }

    @Test
    public void only_return_events_with_a_given_stream_prefix() {
        Filter filter = Filter.STREAM_ID.prefix("stream-a");

        AllEventsSlice slice = eventstore
            .filteredReadAllEventsBackward(Position.END, 4096, false, filter, 4096).join();

        assertThat(
            slice.events.stream().limit(testEvents.size()).map(e -> e.event).collect(toList()),
            containsInOrder(reverse(getEvenIndexedEvent(testEvents))));

    }

    @Test
    public void only_return_events_with_a_given_event_prefix() {
        Filter filter = Filter.EVENT_TYPE.prefix("AE");

        // Have to order the events as we are writing to two streams and can't guarantee ordering

        AllEventsSlice slice = eventstore
            .filteredReadAllEventsBackward(Position.END, 4096, false, filter, 4096).join();
        assertThat(slice.readDirection, is(ReadDirection.Backward));
        assertThat(slice.events.stream().map(e -> e.event).sorted(
            Comparator.comparing(t -> t.eventId)).limit(testEvents.size())
            .collect(toList()), containsInOrder(
            reverse(testEvents.stream().filter(e -> e.type.equals("AEvent")).collect(toList()))));

    }

}
