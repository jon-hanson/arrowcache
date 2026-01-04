package io.nson.arrowcache.common.utils;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.joining;

public abstract class ArrowUtils {
    private ArrowUtils() {}

    public static final Schema EMPTY_SCHEMA = new Schema(
            Collections.emptyList()
    );

    public static Result stringToResult(String s) {
        return new Result(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String resultToString(Result res) {
        return ByteUtils.bytesToString(res.getBody());
    }

    public static String toString(FlightProducer.CallContext context) {
        return "Context{peerIdentity=" + context.peerIdentity() +
                "; isCancelled=" + context.isCancelled()
                +"}";
    }

    public static String toString(Action action) {
        return "Action{type=" + action.getType() +
                "; body=" + ByteUtils.bytesToString(action.getBody())
                +"}";
    }

    public static String textToString(Text text) {
        return text.toString();
    }

    public static boolean schemaMatch(Schema schemaA, Schema schemaB) {
        return schemaA.equals(schemaB);
    }

    public static String toString(Schema schema) {
        return schema.getFields().stream()
                .map(Field::getName)
                .collect(joining(", "));
    }

    public static void toLines(Consumer<String> lineCons, VectorSchemaRoot vsc) {
        lineCons.accept(toString(vsc.getSchema()));
        final List<FieldVector> fvs = vsc.getFieldVectors();
        for (int r = 0; r < vsc.getRowCount(); ++r) {
            final int r2 = r;
            lineCons.accept(
                    fvs.stream()
                            .map(fv -> fv.getObject(r2))
                            .map(Object::toString)
                            .collect(joining(", "))
            );
        }
    }

    public static Object toString(FlightDescriptor descriptor) {
        return "FlightDescriptor{" +
                    "isCommand=" + descriptor.isCommand() +
                    (descriptor.isCommand() ?
                       " command=<" + descriptor.getCommand().length + " bytes>" :
                        " path=<" + descriptor.getPath()
                    ) + "}";
    }
}
