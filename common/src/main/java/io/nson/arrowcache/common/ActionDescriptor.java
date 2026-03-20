package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.DeleteRequest;
import io.nson.arrowcache.common.avro.MergeRequest;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface ActionDescriptor<T extends SpecificRecord> {
    ActionDescriptor<DeleteRequest> DELETE = new Impl<>(
            DeleteRequest.getEncoder(),
            DeleteRequest.getDecoder(),
            "DELETE",
            "Delete table entries");

    ActionDescriptor<MergeRequest> MERGE = new Impl<>(
            MergeRequest.getEncoder(),
            MergeRequest.getDecoder(),
            "MERGE",
            "Merge each table");

    static void forEach(Consumer<ActionDescriptor> cons) {
        Stream.of(DELETE, MERGE).forEach(cons);
    }

    static void forEachType(Consumer<ActionType> cons) {
        forEach(actionDescriptor -> cons.accept(actionDescriptor.actionType()));
    }

    String name();

    ActionType actionType();

    Action createAction(byte[] body);

    T decode(Action action) throws IOException;

    Action encode(T request) throws IOException;

    final class Impl<T extends SpecificRecord> implements ActionDescriptor<T> {
        private final BinaryMessageEncoder<T> encoder;
        private final BinaryMessageDecoder<T> decoder;
        private final String name;
        private final ActionType actionType;

        private Impl(
                BinaryMessageEncoder<T> encoder,
                BinaryMessageDecoder<T> decoder,
                String name,
                String description
        ) {
            this.encoder = encoder;
            this.decoder = decoder;
            this.name = name;
            this.actionType = new ActionType(name, description);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ActionType actionType() {
            return actionType;
        }

        @Override
        public Action createAction(byte[] body) {
            return new Action(name, body);
        }

        @Override
        public T decode(Action action) throws IOException {
            return decoder.decode(action.getBody());
        }

        @Override
        public Action encode(T request) throws IOException {
            return createAction(encoder.encode(request).array());
        }
    }
}
