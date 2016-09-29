package com.orctom.pipeline.stream;

import akka.stream.*;
import akka.stream.javadsl.*;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.util.ByteString;

import java.nio.file.Paths;
import java.math.BigInteger;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

public class StreamTest {

  public static void main(String[] args) {
    new StreamTest().testStream();
  }

  public void testStream() {
    final ActorSystem system = ActorSystem.create("QuickStart");
    final Materializer materializer = ActorMaterializer.create(system);

    final Source<Integer, NotUsed> source = Source.range(1, 100);
    source.runForeach(System.out::println, materializer);

    final Source<BigInteger, NotUsed> factorials = source
            .scan(BigInteger.ONE, (acc, next) -> acc.multiply(BigInteger.valueOf(next)));

//    final CompletionStage<IOResult> result = factorials
//            .map(num -> ByteString.fromString(num.toString() + "\n"))
//            .runWith(FileIO.toPath(Paths.get("factorials.txt")), materializer);
//
//    factorials.map(BigInteger::toString).runWith(lineSink("factorial2.txt"), materializer);

    final CompletionStage<Done> done =
        factorials
            .zipWith(Source.range(0, 20), (num, idx) -> String.format("%d! = %s", idx, num))
//            .throttle(1, Duration.create(1, TimeUnit.SECONDS), 1, ThrottleMode.shaping())
            .runForeach(System.out::println, materializer);
  }

  public Sink<String, CompletionStage<IOResult>> lineSink(String filename) {
    return Flow.of(String.class)
        .map(s -> ByteString.fromString(s.toString() + "\n"))
        .toMat(FileIO.toPath(Paths.get(filename)), Keep.right());
  }
}
