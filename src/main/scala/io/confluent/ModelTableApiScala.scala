package io.confluent

import org.apache.flink.table.api.{EnvironmentSettings, Model, ModelDescriptor, Schema, Table, TableEnvironment}
import org.apache.flink.table.api.DataTypes.{FIELD, ROW, STRING}
import org.apache.flink.table.api.Expressions.row
import org.apache.flink.types.ColumnList

import scala.collection.JavaConverters.mapAsJavaMapConverter


object ModelTableApiScala {

  def main(args: Array[String]): Unit = {
    // 1. Set up the local environment
    val settings = EnvironmentSettings.inStreamingMode()
    val tEnv = TableEnvironment.create(settings)

    // 2. Create a source table from in-memory data
    // This is the Scala equivalent of from_elements
    val myTable: Table =
      tEnv.fromValues(
        ROW(FIELD("text", STRING())),
        row("Hello"),
        row("Machine Learning"),
        row("Good morning"))

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
        .build())

    val model = tEnv.fromModel("my_model")

    // 4. Use the model to make predictions
    val predictResult = model.predict(myTable, ColumnList.of("text"))

    // 5. Print the results to your console
    // .execute() runs the job and .print() displays the results
    println("--- Flink Job Results ---")
    predictResult.execute().print()
    println("-------------------------")

    // 6. Async prediction example
    val asyncPredictResult =
      model.predict(myTable, ColumnList.of("text"), Map("async" -> "true").asJava)
    println("--- Flink Job Results ---")
    asyncPredictResult.execute().print()
    println("-------------------------")
  }
}

