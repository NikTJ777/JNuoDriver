package com.nuodb.sales.jnuodriver;

public class Main {

    public static void main(String[] args) {
        try (Controller controller = new Controller()) {
            controller.configure(args);
            controller.init();
            controller.run();
        }
        catch (InterruptedException e) {
            System.out.println("JNuoTest interrupted - exiting");
        }
        catch (Exception e) {
            System.out.println("Exiting with fatal error: " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
}
