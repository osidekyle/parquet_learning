import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.checkerframework.checker.units.qual.C;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

public class yuh {
    public void yuh(){
        Schema.Parser parser=new Schema.Parser();
        try {
            Schema schema = parser.parse(getClass().getResourceAsStream("schema.avsc"));
            GenericRecord datum=new GenericData.Record(schema);
            datum.put("left","L");
            datum.put("right","R");

            Path path = new Path("data.parquet");
            AvroParquetWriter<GenericRecord> writer= new AvroParquetWriter<GenericRecord>(path, schema);
            writer.write(datum);
            writer.close();


            /*AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(path);
            GenericRecord result = reader.read();
*/


        }
        catch(IOException ex){
            System.out.println("darn");
        }



    }
    public void yuh2(){
        try {
            Path path = new Path("data.parquet");

            AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(path);
            GenericRecord result = reader.read();
        }
        catch(IOException ex){
            System.out.println("Real darn");
        }
    }public void yuh3(){
        try {
            Schema projectionSchema=new Schema.Parser().parse(
                    getClass().getResourceAsStream("ProjectedStringPair.avsc")
            );
            Configuration conf = new Configuration();
            AvroReadSupport.setRequestedProjection(conf,projectionSchema);

            Path path = new Path("ProjectedStringPair.avsc");

            AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(conf,path);
            GenericRecord result=reader.read();

        }
        catch(IOException ex){
            System.out.println("Real darn");
        }
    }
}
