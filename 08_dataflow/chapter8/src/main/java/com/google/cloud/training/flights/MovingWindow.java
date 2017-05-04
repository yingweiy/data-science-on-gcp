package com.google.cloud.training.flights;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

@SuppressWarnings("serial")
public class MovingWindow extends NonMergingWindowFn<Object, IntervalWindow> {
  private final Duration averagingInterval;
  
  public MovingWindow(Duration averagingInterval) {
    this.averagingInterval = averagingInterval;
  }

  private static final Duration ONE_MSEC = Duration.millis(1);

  public IntervalWindow assignWindow(Instant ts) {
    return new IntervalWindow(ts.minus(averagingInterval), ts.plus(ONE_MSEC));
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
   return equals(other);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }
  
  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    return Collections.singletonList(assignWindow(c.timestamp()));
  }
  
  @Override
  public IntervalWindow getSideInputWindow(BoundedWindow mainWindow) {
    if (mainWindow instanceof GlobalWindow) {
      throw new IllegalArgumentException(
          "Attempted to get side input window for GlobalWindow from non-global WindowFn");
    }
    Instant end = mainWindow.maxTimestamp();
    return new IntervalWindow(end.minus(averagingInterval), end.plus(ONE_MSEC));
  }
  
  public static void main(String[] args) {
    String[] events = {
        "2015-01-04T01:00:00Z,a,10",
        "2015-01-04T01:10:00Z,b,11",
        "2015-01-04T01:20:00Z,a,12",
        "2015-01-04T01:30:00Z,b,13",
        "2015-01-04T01:40:00Z,a,14",
        "2015-01-04T01:50:00Z,b,15",
        "2015-01-04T02:00:00Z,a,16",
    };
    
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
    p //
        .apply("Read", Create.of(Arrays.asList(events))) //
        .apply("Emit", ParDo.of(new DoFn<String, KV<String, Double>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            String line = c.element();
            String[] pieces = line.split(",");
            c.outputWithTimestamp(KV.of(pieces[1], Double.valueOf(pieces[2])), Instant.parse(pieces[0]));
          }
        })) //
        .apply(Window.<KV<String, Double>> into(new MovingWindow(Duration.standardMinutes(30)))) //
        .apply("Mean", Mean.perKey())
        .apply("Print", ParDo.of(new DoFn<KV<String, Double>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, IntervalWindow w) throws Exception {
            KV<String, Double> result = c.element();
            System.out.println(w.maxTimestamp() + " " + result.getKey() + " " + result.getValue());
          }
        }));
    p.run().waitUntilFinish();
    
    /*
     Expected result should include:
2015-01-04T01:30:00.000Z a (10+12)/2=11
2015-01-04T01:40:00.000Z b (11+13)/2=12
     Actual result:
2015-01-04T01:10:00.000Z b 11.0
2015-01-04T01:00:00.000Z a 10.0
2015-01-04T01:30:00.000Z b 13.0
2015-01-04T01:20:00.000Z a 12.0
2015-01-04T01:50:00.000Z b 15.0
2015-01-04T01:40:00.000Z a 14.0
2015-01-04T02:00:00.000Z a 16.0
     */
  }
  
}
