package arrow;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A Flight client for integration testing.
 */
class SDCFlightClient {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SDCFlightClient.class);

  public static void main(String[] args) {
    final String host = "localhost";
    final int port = 5005;
    final String inputPath = "../data/json_data.json";
    try {
      new SDCFlightClient().run(host, port, inputPath);
    } catch (ParseException e) {
      fatalError("Invalid parameters", e);
    } catch (IOException e) {
      fatalError("Error accessing files", e);
    } catch (Exception e) {
      fatalError("Unknown error", e);
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

  private void run(String host, int port, String inputPath) throws Exception {
    final Location defaultLocation = Location.forGrpcInsecure(host, port);
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final FlightClient client = FlightClient.builder(allocator, defaultLocation).build()) {
        testStream(allocator, defaultLocation, client, inputPath);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void testStream(BufferAllocator allocator, Location server, FlightClient client, String inputPath)
      throws IOException {
    // Read data from JSON and upload to server.
    FlightDescriptor descriptor = FlightDescriptor.path(inputPath);
    try (JsonFileReader reader = new JsonFileReader(new File(inputPath), allocator);
        VectorSchemaRoot root = VectorSchemaRoot.create(reader.start(), allocator)) {
      FlightClient.ClientStreamListener stream = client.startPut(descriptor, root, reader,
          new AsyncPutListener() {
            int counter = 0;

            @Override
            public void onNext(PutResult val) {
              final byte[] metadataRaw = new byte[checkedCastToInt(val.getApplicationMetadata().readableBytes())];
              val.getApplicationMetadata().readBytes(metadataRaw);
              final String metadata = new String(metadataRaw, StandardCharsets.UTF_8);
              if (!Integer.toString(counter).equals(metadata)) {
                throw new RuntimeException(
                    String.format("Invalid ACK from server. Expected '%d' but got '%s'.", counter, metadata));
              }
              counter++;
            }
          });
      int counter = 0;
      while (reader.read(root)) {
        final byte[] rawMetadata = Integer.toString(counter).getBytes(StandardCharsets.UTF_8);
        final ArrowBuf metadata = allocator.buffer(rawMetadata.length);
        metadata.writeBytes(rawMetadata);
        // Transfers ownership of the buffer, so do not release it ourselves
        stream.putNext(metadata);
        root.clear();
        counter++;
      }
      stream.completed();
      // Need to call this, or exceptions from the server get swallowed
      stream.getResult();
    }
  }
}
