package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import tech.stackable.spark.operator.systemd.SparkSystemd;

public class SparkSystemdDoneable extends CustomResourceDoneable<SparkSystemd> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkSystemdDoneable(SparkSystemd systemd, Function function) {
        super(systemd, function);
	}
}

