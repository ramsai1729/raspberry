package com.narsimhulu.aws;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sairam
 */
public class KinesisClient {

  private final AmazonKinesisClient amazonKinesisClient;
  private final String streamName;
  // we have only one shard, so use constant name for now
  private static final String PARTITION_KEY = "Key";
  private static final String shardId = "shardId-000000000000";

  public KinesisClient(String streamName) {
    this.streamName = streamName;
    amazonKinesisClient = new AmazonKinesisClient(new ClasspathPropertiesFileCredentialsProvider());
  }

  public void putRecord(String data) {
    PutRecordRequest putRecordsRequest  = new PutRecordRequest();
    putRecordsRequest.setStreamName(streamName);
    putRecordsRequest.setData(ByteBuffer.wrap(data.getBytes()));
    putRecordsRequest.setPartitionKey(PARTITION_KEY);
    PutRecordResult result = amazonKinesisClient.putRecord(putRecordsRequest);
    System.out.println(result.toString());
  }

  public List<String> getRecords() {

   /* DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    describeStreamRequest.setStreamName( streamName );
    List<Shard> shards = new ArrayList<>();
    describeStreamRequest.setExclusiveStartShardId( null );
    DescribeStreamResult describeStreamResult = amazonKinesisClient.describeStream( describeStreamRequest );
    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
    Shard shard = shards.get(0);*/

    GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    getShardIteratorRequest.setStreamName(streamName);
    getShardIteratorRequest.setShardId(shardId);
    getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

    GetShardIteratorResult getShardIteratorResult = amazonKinesisClient.getShardIterator(getShardIteratorRequest);
    String shardIterator = getShardIteratorResult.getShardIterator();

    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    getRecordsRequest.setShardIterator(shardIterator);
    getRecordsRequest.setLimit(25);

    GetRecordsResult getRecordsResult = amazonKinesisClient.getRecords(getRecordsRequest);
    List<Record> records = getRecordsResult.getRecords();

    List<String> result = new ArrayList<>();
    for(Record r : records) {
      result.add(new String(r.getData().array()));
    }
    return result;
  }

}
