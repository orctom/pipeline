package com.orctom.pipeline.model;

import com.orctom.rmq.Message;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SentMessageTest {

  @Test
  public void test() {
    String id = "10001";
    String payload = "this is the payload";
    Message original = new Message(id, payload.getBytes());

    String role = "dummy";
    SentMessage sentMessage = new SentMessage(role, original);
    System.out.println(new String(original.getData()));
    System.out.println(new String(sentMessage.getData()));
    assertThat(sentMessage.getRole(), equalTo(role));
    assertThat(sentMessage.getOriginalData(), equalTo(payload.getBytes()));
  }
}
