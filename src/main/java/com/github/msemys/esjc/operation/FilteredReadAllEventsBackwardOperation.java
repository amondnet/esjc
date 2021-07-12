package com.github.msemys.esjc.operation;

import static com.github.msemys.esjc.util.Strings.defaultIfEmpty;

import com.github.msemys.esjc.AllEventsSlice;
import com.github.msemys.esjc.Position;
import com.github.msemys.esjc.ReadDirection;
import com.github.msemys.esjc.UserCredentials;
import com.github.msemys.esjc.proto.EventStoreClientMessages.Filter;
import com.github.msemys.esjc.proto.EventStoreClientMessages.FilteredReadAllEvents;
import com.github.msemys.esjc.proto.EventStoreClientMessages.FilteredReadAllEventsCompleted;
import com.github.msemys.esjc.tcp.TcpCommand;
import com.google.protobuf.MessageLite;
import java.util.concurrent.CompletableFuture;

public class FilteredReadAllEventsBackwardOperation extends AbstractOperation<AllEventsSlice, FilteredReadAllEventsCompleted>  {
    private final Position position;
    private final int maxCount;
    private final boolean resolveLinkTos;
    private final boolean requireLeader;
    private final Integer maxSearchWindow;
    private final Filter filter;

    public FilteredReadAllEventsBackwardOperation(CompletableFuture<AllEventsSlice> result,
        Position position,
        int maxCount,
        boolean resolveLinkTos,
        boolean requireLeader,
        Integer maxSearchWindow,
        Filter filter,
        UserCredentials userCredentials) {
        super(result, TcpCommand.ReadAllEventsForward, TcpCommand.ReadAllEventsForwardCompleted, userCredentials);
        this.position = position;
        this.maxCount = maxCount;
        this.resolveLinkTos = resolveLinkTos;
        this.requireLeader = requireLeader;
        this.maxSearchWindow = maxSearchWindow;
        this.filter = filter;
    }
    @Override
    protected MessageLite createRequestMessage() {
        return FilteredReadAllEvents
            .newBuilder()
            .setCommitPosition(position.commitPosition)
            .setPreparePosition(position.preparePosition)
            .setMaxCount(maxCount)
            .setMaxSearchWindow(maxSearchWindow)
            .setResolveLinkTos(resolveLinkTos)
            .setRequireLeader(requireLeader)
            .setFilter(filter).build();
    }

    @Override
    protected FilteredReadAllEventsCompleted createResponseMessage() {
        return FilteredReadAllEventsCompleted.getDefaultInstance();
    }

    @Override
    protected InspectionResult inspectResponseMessage(FilteredReadAllEventsCompleted response) {
        switch (response.getResult()) {
            case Success:
                succeed();
                return InspectionResult.newBuilder()
                    .decision(InspectionDecision.EndOperation)
                    .description("Success")
                    .build();
            case Error:
                fail(new ServerErrorException(defaultIfEmpty(response.getError(), "<no message>")));
                return InspectionResult.newBuilder()
                    .decision(InspectionDecision.EndOperation)
                    .description("Error")
                    .build();
            case AccessDenied:
                fail(new AccessDeniedException("Read access denied for $all."));
                return InspectionResult.newBuilder()
                    .decision(InspectionDecision.EndOperation)
                    .description("Error")
                    .build();
            default:
                throw new IllegalArgumentException(String.format("Unexpected ReadAllResult: %s.", response.getResult()));
        }    }

    @Override
    protected AllEventsSlice transformResponseMessage(FilteredReadAllEventsCompleted response) {
        return new AllEventsSlice(
            ReadDirection.Backward,
            new Position(response.getCommitPosition(), response.getPreparePosition()),
            new Position(response.getNextCommitPosition(), response.getNextPreparePosition()),
            response.getEventsList(), response.getIsEndOfStream());
    }
}
