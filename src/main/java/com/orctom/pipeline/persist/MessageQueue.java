package com.orctom.pipeline.persist;

import com.orctom.pipeline.model.SentMessage;
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

  private MessageQueue(RMQOptions options) {
    super(options);
  }

  public static MessageQueue getInstance(RMQOptions options) {
    return (MessageQueue) RMQ.getInstance(options, new MessageQueue(options));
  }

  public void iterateSentMessages(Consumer<SentMessage> consumer) {
    try {
      LOGGER.trace("Start resending un-acked messages...");
      RocksIterator messageIterator = super.iter(Q_SENT);
      if (null == messageIterator) {
        return;
      }
      messageIterator.seekToFirst();
      Message message = getNextMessage(messageIterator);
      if (null == message) {
        LOGGER.trace("Done, no sent messages.");
        return;
      }

      RocksIterator sentMessagesIterator = super.iter(Q_SENT_RECORDS);
      if (null == sentMessagesIterator) {
        clearSentQueue();
        LOGGER.trace("Done, no sent records, sent messages got cleared.");
        return;
      }
      sentMessagesIterator.seekToFirst();
      SentMessage sentMessage = getNextSentMessage(sentMessagesIterator);
      if (null == sentMessage) {
        clearSentQueue();
        LOGGER.trace("Done, no sent records, sent messages got cleared.");
        return;
      }

      boolean hasNotFoundMatches = true;
      while (null != message && null != sentMessage) {
        long originalId = Long.valueOf(message.getId());
        long recordId = sentMessage.getOriginalId();
        LOGGER.trace("originalId: {}, recordId {}", originalId, sentMessage.getId());
        if (originalId == recordId) {
          sentMessage.setData(message.getData());
          consumer.accept(sentMessage);
          hasNotFoundMatches = false;
          LOGGER.trace("Resent: {}", recordId);
          message = getNextMessage(messageIterator);
          sentMessage = getNextSentMessage(sentMessagesIterator);

        } else {
          if (hasNotFoundMatches) {
            delete(Q_SENT, message.getId());
            LOGGER.trace("Deleted from [sent]: {}", originalId);
          }

          if (originalId < recordId) {
            LOGGER.trace("sent: {}, record {}, 'sent' move next.", originalId, recordId);
            message = getNextMessage(messageIterator);
          } else {
            LOGGER.trace("sent: {}, record {}, 'record' move next.", originalId, recordId);
            sentMessage = getNextSentMessage(sentMessagesIterator);
          }
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

  private SentMessage getNextSentMessage(RocksIterator sentMessagesIterator) {
    sentMessagesIterator.next();
    if (!sentMessagesIterator.isValid()) {
      return null;
    }

    String sentId = new String(sentMessagesIterator.key());
    String[] items = sentId.split(AT_SIGN);
    if (2 != items.length) {
      LOGGER.error("Wrong id, expected {}, id: {}", AT_SIGN, sentId);
      return getNextSentMessage(sentMessagesIterator);
    }
    long originalId = Long.valueOf(items[0]);
    String role = items[1];
    return new SentMessage(role, originalId, sentId);
  }

  private void clearSentQueue() {
    flush(Q_SENT);
  }
}
