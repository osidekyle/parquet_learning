import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;


public class Main {
    public static void main(String[] args){
        MessageType schema = MessageTypeParser.parseMessageType(
                "Message Pair {\n"+
                        "  required binary left (UTF8);\n"+
                        "  required binary right (UTF8);\n"+
                        "}"
        );

        GroupFactory groupFactory=new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup().append("left","L").append("right","R");


        Configuration conf =new Configuration();
        Path path = new Path("data.parquet");
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema,conf);
        try {
            ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf);

            writer.write(group);
            writer.close();

            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader<Group> reader = new ParquetReader<Group>(path, readSupport);

            Group result = reader.read();
            System.out.println(result.getString("left",0));
            System.out.println(result.getString("right",0));

        }
        catch(IOException ex){
            System.out.println("darn");
        }


    }
}
