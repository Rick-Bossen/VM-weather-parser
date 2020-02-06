/**
 * Main class that starts the {@link Parser}.
 *
 * @author Rick
 * @author Martijn
 */
public class Main {

    private static final String PATH = "/var/nfs/";

    public static void main(String[] args) {
        new Parser(PATH).run();
    }
}
