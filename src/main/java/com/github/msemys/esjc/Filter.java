package com.github.msemys.esjc;

import com.github.msemys.esjc.proto.EventStoreClientMessages;
import com.github.msemys.esjc.proto.EventStoreClientMessages.Filter.FilterType;
import java.util.Arrays;
import java.util.Collections;

/**
 * A Filter, used to filter events when reading from the $all stream.
 */
public class Filter {

    private final EventStoreClientMessages.Filter filter;

    public Filter(
        EventStoreClientMessages.Filter.FilterContext context,
        EventStoreClientMessages.Filter.FilterType type, Iterable<String> data) {
        this.filter = EventStoreClientMessages.Filter.newBuilder().setContext(context)
            .setType(type).addAllData(data).build();
    }

    public EventStoreClientMessages.Filter getValue() {
        return filter;
    }

    /**
     * Filters by EventType.
     */
    public static final Filter.FilterContext EVENT_TYPE = new FilterContext(EventStoreClientMessages.Filter.FilterContext.EventType);

    /**
     * Filters by StreamId
     */
    public static final Filter.FilterContext STREAM_ID = new FilterContext(EventStoreClientMessages.Filter.FilterContext.StreamId);

    public static final Filter EXCLUDE_SYSTEM_EVENTS = new Filter(EventStoreClientMessages.Filter.FilterContext.EventType,
        FilterType.Regex,
        Collections.singletonList("/^[^\\$].*/"));

    /**
     * A filter context
     */
    public static class FilterContext {

        private final EventStoreClientMessages.Filter.FilterContext context;

        FilterContext(
            EventStoreClientMessages.Filter.FilterContext context) {
            this.context = context;
        }

        public Filter regex(String regex) {
            return new Filter(context, FilterType.Regex, Collections.singletonList(regex));
        }

        public Filter prefix(String... prefixes) {
            return new Filter(context, FilterType.Prefix, Arrays.asList(prefixes));
        }

        public Filter prefix(Iterable<String> prefixes) {
            return new Filter(context, FilterType.Prefix, prefixes);
        }
    }
}
