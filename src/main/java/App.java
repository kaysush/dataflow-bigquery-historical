import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class App {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        final String projectId = "just-landing-231706";
        final String dataset = "test_dataset";
        final String historicalTable = "historical_table";
        final String newTable = "new_table";
        pipeline.apply("Read from BigQuery", BigQueryIO
                .readTableRows()
                .from(String.format("%s:%s.%s", projectId, dataset, historicalTable)))
                .apply("Write to BigQuery", BigQueryIO.<TableRow>write().to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
                    @Override
                    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
                        try {
                            TableRow tr = input.getValue();
                            String timeStamp = (String) tr.get("client_timestamp");
                            SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
                            Date d = inputDateFormat.parse(timeStamp);
                            SimpleDateFormat partitionDateFormat = new SimpleDateFormat("yyyyMMdd");
                            String partitionDate = partitionDateFormat.format(d);
                            String tableName = String.format("%s:%s.%s$%s", projectId, dataset, newTable, partitionDate);
                            return new TableDestination(tableName, null);
                        } catch (ParseException ex) {
                            ex.printStackTrace();
                        }
                        return null;
                    }
                }).withFormatFunction(new SerializableFunction<TableRow, TableRow>() {
                    @Override
                    public TableRow apply(TableRow input) {
                        int id = Integer.parseInt(input.get("id").toString());
                        String attributesStr = input.get("attributes").toString();
                        JsonObject obj = new JsonParser().parse(attributesStr).getAsJsonObject();
                        String name = obj.get("name").getAsString();
                        int age = obj.get("age").getAsInt();
                        Object timeStamp = input.get("client_timestamp");
                        TableRow output = new TableRow();
                        TableRow attributes = new TableRow();
                        attributes.put("age", age);
                        attributes.put("name", name);
                        output.put("id", id);
                        output.put("attributes", attributes);
                        output.put("client_timestamp", timeStamp);
                        return output;
                    }
                }).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));


        pipeline.run();
    }
}
