package consumer.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/**
 * Extracts the kafka-paritition-number and largest-offset-read-for-that-partition from the kafka-receiver output
 */
public class PartitionOffsetPair<E> implements PairFlatMapFunction<Iterator<MessageAndMetadata<E>>, Integer, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionOffsetPair.class);

    @Override
    public Iterator<Tuple2<Integer, Long>> call(Iterator<MessageAndMetadata<E>> it) throws Exception {
        MessageAndMetadata<E> mmeta = null;
        while (it.hasNext()) {
            mmeta = it.next();
            LOG.debug("Consumed partition = {}, offset = {}", mmeta.getPartition(), mmeta.getOffset());
        }
        // Return the kafka-partition-number and the largest offset read
        List<Tuple2<Integer, Long>> kafkaPartitionToOffsetList = new ArrayList<>(1);
        if (mmeta != null) {
            LOG.debug("selected largest offset {} for partition {}", mmeta.getOffset(), mmeta.getPartition());
            kafkaPartitionToOffsetList.add(new Tuple2<>(mmeta.getPartition().partition, mmeta.getOffset()));
        }
        return kafkaPartitionToOffsetList.iterator();
    }
}

