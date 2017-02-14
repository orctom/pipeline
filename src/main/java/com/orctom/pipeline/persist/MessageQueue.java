package com.orctom.pipeline.persist;

import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.model.SentMessage;
import com.orctom.pipeline.util.IdUtils;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQ;
import com.orctom.rmq.RMQOptions;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.orctom.pipeline.Constants.*;

public class MessageQueue extends RMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueue.class);
  private static final long SAFE_TIME_SPAN = 1_310_720_000;// about 10  seconds

  private SimpleMetrics metrics;

  private MessageQueue(RMQOptions options, SimpleMetrics metrics) {
    super(options);
    this.metrics = metrics;
  }

  public static MessageQueue getInstance(RMQOptions options, SimpleMetrics metrics) {
    return (MessageQueue) RMQ.getInstance(options, new MessageQueue(options, metrics));
  }

  public void iterateSentMessages(Consumer<SentMessage> consumer) {
    try {
      long timestamp = IdUtils.generateLong();
      LOGGER.trace("Start resending un-acked messages...");
      RocksIterator messageIterator = super.iter(Q_SENT);
      if (null == messageIterator) {
        return;
      }
      Message message = getNextMessage(messageIterator);
      if (null == message) {
        LOGGER.trace("Done, no sent messages.");
        return;
      }

      RocksIterator sentRecordsIterator = super.iter(Q_SENT_RECORDS);
      if (null == sentRecordsIterator) {
        clearSentQueue();
        LOGGER.debug("Done, no sent records, sent messages got cleared.");
        return;
      }
      SentMessage sentRecord = getNextSentRecord(sentRecordsIterator);
      if (null == sentRecord) {
        clearSentQueue();
        LOGGER.debug("Done, empty sent records, sent messages got cleared.");
        return;
      }

      boolean isIdle = true;
      while (null != message && null != sentRecord) {
        try {
          long originalId = Long.valueOf(message.getId());
          if (timestamp - originalId < SAFE_TIME_SPAN) {
            return;
          }

          long recordId = sentRecord.getOriginalId();
          LOGGER.trace("originalId: {}, recordId {}", originalId, sentRecord.getId());
          if (originalId == recordId) {
            sentRecord.setData(message.getData());
            consumer.accept(sentRecord);
            metrics.mark(METER_SENT);
            LOGGER.trace("Resent: {}", recordId);
            sentRecord = getNextSentRecord(sentRecordsIterator);
            isIdle = false;

          } else {
            if (originalId < recordId) {
              LOGGER.trace("sent: {}, record {}, 'sent' move next.", originalId, recordId);
              if (isIdle) {
                super.delete(Q_SENT, message.getId());
              }
              message = getNextMessage(messageIterator);

            } else {
              LOGGER.trace("sent: {}, record {}, 'record' move next.", originalId, recordId);
              if (isIdle) {
                super.delete(Q_SENT_RECORDS, sentRecord.getId());
              }
              sentRecord = getNextSentRecord(sentRecordsIterator);
            }
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private Message getNextMessage(RocksIterator messageIterator) {
    messageIterator.next();
    if (!messageIterator.isValid()) {
      return null;
    }

    String id = new String(messageIterator.key());
    byte[] data = messageIterator.value();
    return new Message(id, data);
  }

  private SentMessage getNextSentRecord(RocksIterator sentMessagesIterator) {
    sentMessagesIterator.next();
    if (!sentMessagesIterator.isValid()) {
      return null;
    }

    String sentId = new String(sentMessagesIterator.key());
    String[] items = sentId.split(AT_SIGN);
    if (2 != items.length) {
      LOGGER.error("Wrong id, expected {}, id: {}", AT_SIGN, sentId);
      return getNextSentRecord(sentMessagesIterator);
    }
    long originalId = Long.valueOf(items[0]);
    String role = items[1];
    return new SentMessage(role, originalId, sentId);
  }

  private void clearSentQueue() {
    super.flush(Q_SENT, IdUtils.generate());
  }
}
