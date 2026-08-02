package org.example.generic;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {
    public static void main(String[] args) {
        System.out.printf("Creating Generic Record Example");

        // 0. define schema

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" + "     \"type\": \"record\",\n" + "     \"namespace\": \"com.example\",\n" + "     \"name\": \"Customer\",\n" + "     \"doc\": \"Avro Schema for our Customer\",     \n" + "     \"fields\": [\n" + "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" + "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" + "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" + "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" + "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" + "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" + "     ]\n" + "}");

        // 1. create generic record
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
        genericRecordBuilder.set("first_name", "john");
        genericRecordBuilder.set("last_name", "doe");
        genericRecordBuilder.set("age", 12);
        genericRecordBuilder.set("height", 170f);
        genericRecordBuilder.set("weight", 83.2f);
        genericRecordBuilder.set("automated_email", false);

        GenericData.Record customer = genericRecordBuilder.build();

        //gives runtime errors
        GenericRecordBuilder genericRecordBuilderDefault = new GenericRecordBuilder(schema);
        genericRecordBuilderDefault.set("first_name", "john");
//        genericRecordBuilderDefault.set("last_name", "doe");
        genericRecordBuilderDefault.set("age", 12);
        genericRecordBuilderDefault.set("height", 170f);
        genericRecordBuilderDefault.set("weight", 83.2f);
//        genericRecordBuilderDefault.set("automated_email", false);



        System.out.println(customer);

        try {
            GenericData.Record customerDefault = genericRecordBuilderDefault.build();
            System.out.println(customerDefault);
        } catch (AvroRuntimeException e){
            System.out.println("Generic Record build did not succeed");
            e.printStackTrace();
        }


        // 2. write that record to file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
            dataFileWriter.close();
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // 3. read that record from a file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){


            // 4. interpret that record
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));

            // read a non existent field
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        }
        catch(IOException e) {
            e.printStackTrace();
        }


    }
}