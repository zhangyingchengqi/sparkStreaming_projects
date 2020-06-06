import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class LogGenerator {

    public static final String FILE_PATH = "./logs/";//文件指定存放的路径

    public static void main(String[] args) throws IOException {
        FileOutputStream outFile = null;
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        Random r = new Random();
        String grades[] = {"[info]", "[warn]", "[error]", "[debug]"};
        String position[] = {"main", "calculate", "saveFile", "openFile"};
        for (int i = 0; i < 5; i++) {
            System.out.println(i);
            String filename = df.format(new Date()) + ".log";
            File file = creatFile(FILE_PATH, filename);
            try {
                outFile = new FileOutputStream(file);
                for (int j = 0; j < 10; j++) {
                    // 日志格式:    [级别]\t位置\tDate:时间\n
                    String log = grades[r.nextInt(grades.length)] + "\t" + position[r.nextInt(position.length)] + "\tDate:" + df.format(new Date()) + "\n";
                    outFile.write(log.getBytes());
                }
                outFile.flush();
                Thread.sleep(r.nextInt(2000)+1000);     //   注意生成的时间  ，上面的文件名是到秒  ，下面的  r.nextInt( xx )有可能是豪秒。
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (outFile != null) {
                    outFile.close();
                }
            }
        }
    }

    public static File creatFile(String filePath, String fileName) {
        File folder = new File(filePath);
        //文件夹路径不存在
        if (!folder.exists() && !folder.isDirectory()) {
            System.out.println("文件夹路径不存在，创建路径:" + filePath);
            folder.mkdirs();
        }
        // 如果文件不存在就创建
        File file = new File(filePath + fileName);
        if (!file.exists()) {
            System.out.println("文件不存在，创建文件:" + filePath + fileName);
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }
}
