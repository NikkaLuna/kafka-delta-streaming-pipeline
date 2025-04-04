from pyspark.sql.types import StructType, StringType, TimestampType

kafka_event_schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("value", StringType())
