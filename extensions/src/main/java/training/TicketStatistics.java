package training;

import java.time.Duration;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

public final class TicketStatistics {
    public static void main(String[] args) {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ticket-statistics-lab-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put("auto.offset.reset", "earliest");
        var builder = new StreamsBuilder();
        var mapper = new ObjectMapper();
        builder.stream("dev.lab.streams.sold.v1", Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((ticketId, json) -> {
                try { return mapper.readTree(json).path("data").path("eventId").asText(); }
                catch (Exception e) { throw new IllegalArgumentException("Vente JSON invalide", e); }
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
            .count(Materialized.as("sales-by-concert"))
            .toStream()
            .selectKey((window, count) -> window.key() + "@" + window.window().start())
            .mapValues(count -> count.toString())
            .to("dev.lab.streams.statistics.v1", Produced.with(Serdes.String(), Serdes.String()));
        var streams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
