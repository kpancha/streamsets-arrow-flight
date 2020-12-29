package arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static arrow.ArrowSchemas.personSchema;

public class FlightClientStreamWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightClientStreamWriter.class);

    private final int chunkSize;
    private final Vectorizer<T> vectorizer;

    public FlightClientStreamWriter(int chunkSize, Vectorizer<T> vectorizer) {
        this.chunkSize = chunkSize;
        this.vectorizer = vectorizer;
    }

    public void write(T[] values) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        Location defaultLocation = Location.forGrpcInsecure("localhost", 5005);
        FlightDescriptor descriptor = FlightDescriptor.path("");
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
              VectorSchemaRoot root = VectorSchemaRoot.create(personSchema(), allocator);
              FlightClient client = FlightClient.builder(allocator, defaultLocation).build()) {

            FlightClient.ClientStreamListener stream = client.startPut(descriptor, root, dictProvider, new AsyncPutListener());
            int i = 0;
            // Write vectorized Persons to client stream in chunks
            while (i < values.length) {
              root.allocateNew();
              int chunkIdx = 0;
              // Load batch of chunkSize or smaller
              while (chunkIdx < chunkSize && i + chunkIdx < values.length) {
                  vectorizer.vectorize(values[i + chunkIdx], chunkIdx, root);
                  chunkIdx++;
              }
              root.setRowCount(chunkIdx);
              System.out.println(root.contentToTSVString());
              stream.putNext();
              i += chunkIdx;
              root.clear();
            }
            stream.completed();
          // Need to  call this, or exceptions from the server get swallowed
            stream.getResult();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface Vectorizer<T> {
        void vectorize(T value, int index, VectorSchemaRoot batch);
    }
}
