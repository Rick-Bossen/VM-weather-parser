public class Main {

    private static final String PATH = "/var/nfs/";

    public static void main(String[] args) {
        new Parser(PATH).run();
    }

}
