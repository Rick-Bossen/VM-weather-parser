import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Parser{

    private String path;
    SimpleDateFormat formatter;

    HashMap<Integer, Object[]> outputStreamHashMap = new HashMap<>();

    public Parser(String path) {
        this.path = path;
        this.formatter = new SimpleDateFormat("yyyy-MM-dd_HH");
    }

    private File readEarliestFile(File[] listOfFiles) {
        Arrays.sort(listOfFiles);
        return listOfFiles[0];
    }

    private void store(File file) throws IOException {
        FileInputStream fin = new FileInputStream(file);
        byte[] data = fin.readAllBytes();
        fin.close();
        file.delete();

        int i = 0;
        while (data.length >= i + 35) {
            byte[] id = Arrays.copyOfRange(data, i, i + 4);
            byte[] unix = Arrays.copyOfRange(data, i + 4, i + 8);
            byte[] restData = Arrays.copyOfRange(data, i + 8, i + 35);

            int stationInt = ByteBuffer.wrap(id).getInt();
            int unixInt = ByteBuffer.wrap(unix).getInt();
            Date time = new java.util.Date((long) unixInt * 1000);
            String[] strTime = formatter.format(time).split("_");
            String strDate = strTime[0];
            int hour = Integer.parseInt(strTime[1]);

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


            stream.writeTo(out);
            stream.close();
            i += 35;
        }
    }

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
