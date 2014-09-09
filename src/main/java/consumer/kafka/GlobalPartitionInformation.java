package consumer.kafka;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Objects;

public class GlobalPartitionInformation implements Iterable<Partition>,
		Serializable {

	private Map<Integer, Broker> partitionMap;

	public GlobalPartitionInformation() {
		partitionMap = new TreeMap<Integer, Broker>();
	}

	public void addPartition(int partitionId, Broker broker) {
		partitionMap.put(partitionId, broker);
	}

	@Override
	public String toString() {
		return "GlobalPartitionInformation{" + "partitionMap=" + partitionMap
				+ '}';
	}

	public Broker getBrokerFor(Integer partitionId) {
		return partitionMap.get(partitionId);
	}

	public List<Partition> getOrderedPartitions() {
		List<Partition> partitions = new LinkedList<Partition>();
		for (Map.Entry<Integer, Broker> partition : partitionMap.entrySet()) {
			partitions.add(new Partition(partition.getValue(), partition
					.getKey()));
		}
		return partitions;
	}

	public Iterator<Partition> iterator() {
		final Iterator<Map.Entry<Integer, Broker>> iterator = partitionMap
				.entrySet().iterator();

		return new Iterator<Partition>() {

			public boolean hasNext() {
				return iterator.hasNext();
			}

			public Partition next() {
				Map.Entry<Integer, Broker> next = iterator.next();
				return new Partition(next.getValue(), next.getKey());
			}

			public void remove() {
				iterator.remove();
			}
		};
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(partitionMap);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final GlobalPartitionInformation other = (GlobalPartitionInformation) obj;
		return Objects.equal(this.partitionMap, other.partitionMap);
	}
}
