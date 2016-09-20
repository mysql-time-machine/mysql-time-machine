import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/5/16.
 */
public class CombineFutureTest {

    @Test
    public void testCombine() throws Exception{

        CompletableFuture<Integer> f1 = CompletableFuture.supplyAsync(() -> {

            System.out.println("F1 started");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("F1 to complete");

            return 1;

        });

        CompletableFuture<Integer> f2 = CompletableFuture.supplyAsync(() -> {

            System.out.println("F2 started");

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("F2 to complete");
            throw new RuntimeException("e");
        } );

        CompletableFuture<Integer> f3 = f1.thenCombine(f2, (x,y) -> x+y);

        f3.whenComplete((x,e)->{ if (e != null ) {System.out.println(e);} else {System.out.println(x);} });

        System.out.println("Wait a bit");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Waiting completed");



        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private  Supplier<Integer>[] suppliers = new Supplier[3];

    @Test
    public void testExecutor(){

        for (int i=0;i<3;i++){
            suppliers[i] = new SlowSupplier(i);
        }

        for (int i=0;i<5;i++){
            runTask(i);
        }

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void runTask(int i){
        CompletableFuture.supplyAsync(suppliers[i%suppliers.length]).thenAccept(System.out::println).thenAccept(this::runTask);
    }

    private void runTask(Void aVoid) {
        CompletableFuture.supplyAsync(suppliers[2]).thenAccept(System.out::println).thenAccept(this::runTask);
    }


    private static class SlowSupplier implements Supplier<Integer>{
        private final int v;
        private SlowSupplier(int v) {
            this.v = v;
        }

        @Override
        public Integer get() {

            System.out.println("Supplying starts " + v);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Supplier ends " + v);
            return v;
        }
    }

    private static class SlowSupplierOrdered implements Supplier<Integer>{

        private final AtomicInteger counter = new AtomicInteger();
        private final Random random = new Random();

        @Override
        public Integer get() {
            int v = counter.incrementAndGet();

            //System.out.println("Ordered starts " + v);
            try {
                Thread.sleep(random.nextInt(3000)+2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //System.out.println("Ordered ends " + v);

            return v;
        }
    }

    private SlowSupplierOrdered s= new SlowSupplierOrdered();

    private static class Task {

        private SlowSupplier s;

        public Task(int v){
            s = new SlowSupplier(-v);
        }

        public CompletableFuture<Integer> execute() {
            return CompletableFuture.supplyAsync(s);
        }

    }

    @Test
    public void testConcurrency(){

        for (int i =0;i<3;i++){
            startTask();
        }

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private AtomicInteger tc = new AtomicInteger();

    private void startTask(){
//        CompletableFuture.supplyAsync(s).thenApply(Task::new).whenComplete( (t,e) -> {
//
//            t.execute().whenComplete((r,e2)->{System.out.println("Task logged"+r);startTask();});
//        } );

        System.out.println("TASK STARTED " + tc.incrementAndGet());

        CompletableFuture.supplyAsync(s).thenApply(Task::new).thenCompose(t->t.execute()).whenComplete((x,t)->startTask()).whenComplete( (x,t)->{ System.out.println("logging "+x+"  "+tc.decrementAndGet()); });

    }

    @Test
    public void testUn(){
        byte b = (byte)0xff;

        long v = b & 0xff;

        System.out.println(v);
    }


}
