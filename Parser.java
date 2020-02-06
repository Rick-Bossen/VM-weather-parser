import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a parser.
 * It separates the binary files from the Pi data parser into all the stations per hour.
 *
 * @author Rick
 * @author Martijn
 */
public class Parser {

    private String path;
    SimpleDateFormat formatter;

    HashMap<Integer, Object[]> outputStreamHashMap = new HashMap<>();

    /**
     * Initializes the parser to a given path.
     * The path must contain the folders parsed_files and temp_files.
     * The parsed_files folder will contain the parsed data.
     * The temp_files folder contains the data to be parsed.
     *
     * @param path The path to use.
     */
    public Parser(String path) {
        this.path = path;
        this.formatter = new SimpleDateFormat("yyyy-MM-dd_HH");
    }

    /**
     * Reads the {@link File} with earliest timestamp.
     *
     * @param listOfFiles The list of files to read.
     * @return The {@link File} with the earliest timestamp.
     */
    private File readEarliestFile(File[] listOfFiles) {
        Arrays.sort(listOfFiles);
        return listOfFiles[0];
    }

    /**
     * Stores all the data from the a file into separate files based on station number, date and time.
     *
     * @param file The {@link File} to be read.
     * @throws IOException Thrown if file can't be read from or be created/written to.
     */
    private void store(File file) throws IOException {
        FileInputStream fin = new FileInputStream(file);
        byte[] data = fin.readAllBytes();
        fin.close();
        file.delete();

        int i = 0;
        while (data.length >= i + 35) { // Loop through all entries, 1 entry = 35 bytes
            // Separate data into 3 groups
            byte[] id = Arrays.copyOfRange(data, i, i + 4);
            byte[] unix = Arrays.copyOfRange(data, i + 4, i + 8);
            byte[] restData = Arrays.copyOfRange(data, i + 8, i + 35);

            // Restore binary data into their respective types
            int stationInt = ByteBuffer.wrap(id).getInt();
            int unixInt = ByteBuffer.wrap(unix).getInt();
            Date time = new java.util.Date((long) unixInt * 1000);
            String[] strTime = formatter.format(time).split("_");
            String strDate = strTime[0];
            int hour = Integer.parseInt(strTime[1]);

            // Write data to ByteArrayOutputStream
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write(unix);
            stream.write(restData);

            FileOutputStream out = null;
            if (outputStreamHashMap.containsKey(stationInt)){
                Object[] map = outputStreamHashMap.get(stationInt);
                out = (FileOutputStream) map[1];
                if (hour != (int)map[0]){
                    out.close();
                    outputStreamHashMap.remove(stationInt);
                }
            }

            // Create new folders and files
            if (!outputStreamHashMap.containsKey(stationInt)){
                String filepath = path + "parsed_files/" + stationInt + "/" + strDate + "/" + hour;
                File finalFile = new File(filepath);
                if (!finalFile.exists()) {
                    finalFile.getParentFile().mkdirs();
                    finalFile.createNewFile();
                }

                out = new FileOutputStream(finalFile,true);
                outputStreamHashMap.put(stationInt, new Object[]{hour, out});
            }

            // Write data
            stream.writeTo(out);
            stream.close();
            i += 35;
        }
    }

    /**
     * Runs the parser.
     */
    public void run() {
        while (true) {
            File folder = new File(path + "temp_files/");
            File[] listOfFiles = folder.listFiles();
            if(listOfFiles != null && listOfFiles.length > 1) {
                File file = readEarliestFile(listOfFiles);
                try {
                    store(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    var set = outputStreamHashMap.entrySet();
                    outputStreamHashMap.clear();
                    for(Map.Entry<Integer, Object[]> entry : set) {
                        Object[] map = entry.getValue();
                        ((FileOutputStream) map[1]).close();
                    }
                    Thread.sleep(100);
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
