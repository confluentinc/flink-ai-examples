package io.confluent;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Model;
import org.apache.flink.table.api.ModelDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.ColumnList;

import java.util.Map;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.row;

public class ModelTableApi {

    public static void main(final String[] args) {
        // 1. Set up the local environment
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 2. Create a source table from in-memory data
        // This is the Java equivalent of from_elements
        Table myTable =
                tEnv.fromValues(
                        ROW(FIELD("text", STRING())),
                        row("Hello"),
                        row("Machine Learning"),
                        row("Good morning"));

        // 3. Create model
        tEnv.createModel(
                "my_model",
                ModelDescriptor.forProvider("openai")
                        .inputSchema(Schema.newBuilder().column("i", STRING()).build())
                        .outputSchema(Schema.newBuilder().column("o", STRING()).build())
                        .option("endpoint", "https://api.openai.com/v1/chat/completions")
                        .option("model", "gpt-4.1")
                        .option("system-prompt", "translate to chinese")
                        .option("api-key", "<your-openai-api-key-here>")
                        .build());

        Model model = tEnv.fromModel("my_model");

        // 4. Use the model to make predictions
        Table predictResult = model.predict(myTable, ColumnList.of("text"));

        // 5. Print the results to your console
        // .execute() runs the job and .print() displays the results
        System.out.println("--- Flink Job Results ---");
        predictResult.execute().print();
        System.out.println("-------------------------");

        // 6. Async prediction example
        Table asyncPredictResult =
                model.predict(myTable, ColumnList.of("text"), Map.of("async", "true"));
        System.out.println("--- Flink Job Results ---");
        asyncPredictResult.execute().print();
        System.out.println("-------------------------");
    }
}
