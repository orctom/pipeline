package com.orctom.pipeline;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.ActorRef;

public class HelloWorld extends UntypedActor {
  @Override
  public void unhandled(Object message) {
    super.unhandled(message);
  }

  @Override
  public void preStart() {
    // create the greeter actor
    final ActorRef greeter = getContext().actorOf(Props.create(Greeter.class), "greeter");
    // tell it to perform the greeting
    greeter.tell(Greeter.Msg.GREET, getSelf());
    greeter.tell("hello world test", getSelf());
    greeter.tell(new EventMessage("111", "test"), getSelf());
  }

  @Override
  public void onReceive(Object msg) {
    System.out.println("<HelloWorld>: " + msg);
    if (msg == Greeter.Msg.DONE) {
      // when the greeter is done, stop this actor and with it the application
      getContext().stop(getSelf());
    } else
      unhandled(msg);
  }
}