import com.booking.validator.service.utils.ConcurrentPipeline;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/7/16.
 */
public class ConcurrentPipelineTest {

    @Test
    public void testLimit(){

        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger taskCounter = new AtomicInteger();

        final Random r = new Random();

        ConcurrentPipeline<Integer> pipe = new ConcurrentPipeline(

                ()-> {
                    int c = taskCounter.incrementAndGet();
                    System.out.println("Supply Task "+c+" concurrency " + counter.incrementAndGet());

                    try {
                        Thread.sleep(r.nextInt(1000)+1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (r.nextInt(10)<2){
                        throw new RuntimeException("Supplier error" + c);
                    }

                        return new Supplier<CompletableFuture>() {
                            @Override
                            public CompletableFuture get() {
                                return CompletableFuture.supplyAsync(()->{

                                    try {
                                        Thread.sleep(r.nextInt(4000)+1000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }

                                    if (r.nextInt(10)<2){
                                        throw new RuntimeException( "Transformer error "+ c );
                                    }

                                    return c;});
                            }
                        };

                },
                x -> {counter.decrementAndGet(); System.out.println("Task completed: "+x);} ,

                x -> {counter.decrementAndGet(); System.out.println("Task error: "+x);},
                2);

        pipe.start();

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
