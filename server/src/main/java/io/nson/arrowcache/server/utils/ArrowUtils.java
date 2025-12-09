package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.utils.ByteUtils;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class ArrowUtils {
    private ArrowUtils() {}

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

    public static StringBuilder toString(StringBuilder sb, Schema schema) {
        schema.getFields().forEach(f -> {
            sb.append(f.getName());
            sb.append(", ");
        });
        return sb.append("\n");
    }

    public static StringBuilder toString(StringBuilder sb, VectorSchemaRoot vsc) {
        toString(sb, vsc.getSchema());
        final List<FieldVector> fvs = vsc.getFieldVectors();
        for (int r = 0; r < vsc.getRowCount(); ++r) {
            for (FieldVector fv : fvs) {
                sb.append(fv.getObject(r));
                sb.append(", ");
            }
            sb.append("\n");
        }
        return sb;
    }

    public static String toString(VectorSchemaRoot vsc) {
        return toString(new StringBuilder(), vsc).toString();
    }
}
