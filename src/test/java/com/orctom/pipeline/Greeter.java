package com.orctom.pipeline;

import akka.actor.UntypedActor;

import java.awt.*;

public class Greeter extends UntypedActor {

  public static enum Msg {
    GREET, DONE;
  }

  @Override
  public void onReceive(Object msg) {
    System.out.println("<<<<-----");
    System.out.println(msg);
    System.out.println("----->>>>>");
    if (msg == Msg.GREET) {
      System.out.println("Hello World!");
      getSender().tell(Msg.DONE, getSelf());
    } else if (msg instanceof EventMessage) {
      EventMessage event = (EventMessage) msg;
      System.out.println(event.getId());
      System.out.println(event.getName());
    } else {
      unhandled(msg);
    }
  }
}