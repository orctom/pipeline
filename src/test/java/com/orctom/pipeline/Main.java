package com.orctom.pipeline;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Main {

  public static void main(String[] args) {
    Config config = ConfigFactory.parseString("akka.cluster.roles = [test]").
        withFallback(ConfigFactory.load());

    ActorSystem system = ActorSystem.create("Hello", config);
    ActorRef a = system.actorOf(Props.create(HelloWorld.class), "helloWorld");
    system.actorOf(Props.create(Terminator.class, a), "terminator");
  }

  public static class Terminator extends UntypedActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final ActorRef ref;

    public Terminator(ActorRef ref) {
      this.ref = ref;
      getContext().watch(ref);
    }

    @Override
    public void onReceive(Object msg) {
      if (msg instanceof Terminated) {
        log.info("{} has terminated, shutting down system", ref.path());
        getContext().system().shutdown();
      } else {
        unhandled(msg);
      }
    }

  }
}