# Deploy Flink SQL Statements using Confluent Flink
# Using Confluent Cloud native AI model inference with AWS Bedrock
# Based on: https://docs.confluent.io/cloud/current/ai/ai-model-inference.html
# 
# This file uses infrastructure outputs from the infrastructure Terraform configuration
# via remote state. See main.tf for remote state configuration.
#
# Note: Execution properties (checkpointing interval, parallelism) are typically set via Flink configuration
# or can be set in the properties block. SET statements in Flink SQL statements are not always supported
# in the same way. We'll rely on the properties and Flink's default configuration.
# If needed, these can be configured at the compute pool level or via Flink configuration files.

# Create AWS Bedrock connection using confluent_flink_connection resource
# Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/core/main.tf
resource "confluent_flink_connection" "bedrock_connection" {
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    # Use Flink API key from infrastructure (owned by service account)
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  display_name = "bedrock-connection"
  type         = "BEDROCK"
  # Endpoint must include model path: /model/{model-id}/invoke
  # Format: https://bedrock-runtime.{region}.amazonaws.com/model/{model-id}/invoke
  endpoint = "https://bedrock-runtime.${var.aws_region}.amazonaws.com/model/${var.bedrock_model_id}/invoke"

  # AWS credentials - Using dedicated IAM user (not personal credentials)
  # The IAM user is created in infrastructure/main.tf with minimal Bedrock permissions
  # Credentials are automatically pulled from infrastructure outputs
  # This is more secure than using personal AWS credentials
  aws_access_key    = local.aws_access_key_id
  aws_secret_key    = local.aws_secret_access_key
  aws_session_token = null # IAM user doesn't need session token

  lifecycle {
    create_before_destroy = false
  }
}

# Drop existing LLM model if it exists (to handle version limit and allow recreation)
resource "confluent_flink_statement" "drop_llm_model" {
  statement_name = "drop-llm-textgen-model"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = "DROP MODEL IF EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mdm_fhir_conv_model`;"

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create LLM model for text generation using Bedrock
# Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/core/main.tf
# Assumes mcp_server_endpoint is always present, so MCP connection is always included
locals {
  # Build model statement with MCP connection
  # Reference: https://docs.confluent.io/cloud/current/ai/streaming-agents/call-tools.html#streaming-agents-call-tools
  llm_model_statement = join("", [
    "CREATE MODEL `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mdm_fhir_conv_model` ",
    "INPUT (prompt STRING) OUTPUT (response STRING) ",
    "WITH ( ",
    "'provider' = 'bedrock', ",
    "'task' = 'text_generation', ",
    "'bedrock.connection' = '${confluent_flink_connection.bedrock_connection.display_name}', ",
    "'bedrock.params.max_tokens' = '50000', ",
    "'mcp.connection' = 'agentcore-mcp-server-connection' ",
    ");"
  ])
}

resource "confluent_flink_statement" "mdm_fhir_conv_model_aws" {
  statement_name = "create-llm-textgen-model"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    # Use Flink API key from infrastructure (owned by service account)
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  # Set catalog and database context using properties (per Confluent quickstart pattern)
  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  # CREATE MODEL statement per Confluent quickstart pattern
  # Using fully qualified name: catalog.database.model_name
  # Includes MCP connection for tool access
  # Reference: https://docs.confluent.io/cloud/current/ai/streaming-agents/call-tools.html#streaming-agents-call-tools
  statement = local.llm_model_statement

  depends_on = [
    confluent_flink_connection.bedrock_connection,
    confluent_flink_statement.create_mcp_connection,
    confluent_flink_statement.drop_llm_model
  ]

  lifecycle {
    # Allow statement changes so the model can be updated
  }
}

# Drop existing MDM source table if it exists (to fix schema mismatch)
resource "confluent_flink_statement" "drop_mdm_source_table" {
  statement_name = "drop-table-mdm-source"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = "DROP TABLE IF EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.hl7_mdm}`;"

  depends_on = [
    confluent_flink_statement.mdm_fhir_conv_model_aws
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create source table for HL7 MDM EDI messages
# Uses Schema Registry JSON schema with fields:
# - edi_type: string (required)
# - edi_timestamp: string with date-time format (optional)
# - raw_edi_payload: string (required) - contains the HL7 MDM EDI message
# The schema is registered in Schema Registry and linked to the hl7-mdm-messages topic
locals {
  # Note: Confluent Flink's confluent connector automatically handles format based on catalog/database
  # No explicit format specification needed - the connector infers it from the catalog context
  # Schema validation is handled at the Schema Registry level for governance
  mdm_table_statement = join("", [
    "CREATE TABLE `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.hl7_mdm}` (\n",
    "  message_key STRING,\n",
    "  edi_type STRING,\n",
    "  edi_timestamp STRING,\n",
    "  raw_edi_payload STRING,\n",
    "  message_headers MAP<STRING, STRING> METADATA FROM 'headers',\n",
    "  message_timestamp TIMESTAMP(3) METADATA FROM 'timestamp'\n",
    ");"
  ])
}

resource "confluent_flink_statement" "create_mdm_source_table" {
  statement_name = "create-table-mdm-source"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = local.mdm_table_statement

  depends_on = [
    confluent_flink_statement.drop_mdm_source_table
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Drop existing FHIR sink table if it exists (to fix schema mismatch)
resource "confluent_flink_statement" "drop_fhir_sink_table" {
  statement_name = "drop-table-fhir-sink"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = "DROP TABLE IF EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.output}`;"

  depends_on = [
    confluent_flink_statement.create_mdm_source_table
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create sink table for FHIR JSON messages
resource "confluent_flink_statement" "create_fhir_sink_table" {
  statement_name = "create-table-fhir-sink"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = <<-SQL
CREATE TABLE `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.output}` (
  message_key STRING,
  fhir_json STRING,
  processing_timestamp TIMESTAMP(3)
);
SQL

  depends_on = [
    confluent_flink_statement.drop_fhir_sink_table
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Process MDM EDI messages: convert to FHIR JSON using Bedrock and write to FHIR topic
resource "confluent_flink_statement" "mdm_to_fhir_conversion" {
  statement_name = "mdm-to-fhir-conversion"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = <<-SQL
INSERT INTO `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.output}`
SELECT 
  ms.message_key,
  ml.response AS fhir_json,
  CURRENT_TIMESTAMP AS processing_timestamp
FROM `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.hl7_mdm}` ms,
LATERAL TABLE(ML_PREDICT('mdm_fhir_conv_model', 
  CONCAT('Extract medication information from the following HL7 MDM EDI message. Return a JSON object with two arrays: "medications_to_add" and "medications_to_stop". For each medication, include all relevant details (name, dosage, frequency, route, etc.) in FHIR R4 MedicationStatement or MedicationRequest format. If a medication is being discontinued, include it in "medications_to_stop" with a status of "stopped" or "discontinued". Return only valid JSON without markdown formatting:\n\n', ms.raw_edi_payload)
)) AS ml
WHERE ms.raw_edi_payload IS NOT NULL;
SQL

  depends_on = [
    confluent_flink_statement.create_mdm_source_table,
    confluent_flink_statement.create_fhir_sink_table,
    confluent_flink_statement.mdm_fhir_conv_model_aws
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Drop existing MCP response sink table if it exists
resource "confluent_flink_statement" "drop_mcp_response_sink_table" {
  statement_name = "drop-table-mcp-response-sink"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = "DROP TABLE IF EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-responses`;"

  depends_on = [
    confluent_flink_statement.create_fhir_sink_table
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create sink table for MCP response messages
resource "confluent_flink_statement" "create_mcp_response_sink_table" {
  statement_name = "create-table-mcp-response-sink"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = <<-SQL
CREATE TABLE `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-responses` (
  message_key STRING,
  input_medications_json STRING,
  tool_response STRING,
  processing_timestamp TIMESTAMP(3)
);
SQL

  depends_on = [
    confluent_flink_statement.drop_mcp_response_sink_table
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Process medications from FHIR JSON: invoke MCP tool to update FHIR repository
# BACKUP: Commented out to use agent-based approach instead
# resource "confluent_flink_statement" "process_medications" {
#   statement_name = "process-medications-mcp"
#   organization {
#     id = local.flink_common_config.organization.id
#   }
#   environment {
#     id = local.flink_common_config.environment.id
#   }
#   compute_pool {
#     id = local.flink_common_config.compute_pool.id
#   }
#   principal {
#     id = local.flink_common_config.principal.id
#   }
#   rest_endpoint = local.flink_common_config.rest_endpoint
#   credentials {
#     key    = local.flink_common_config.credentials.key
#     secret = local.flink_common_config.credentials.secret
#   }
#
#   properties = {
#     "sql.current-catalog"  = local.environment_display_name
#     "sql.current-database" = local.kafka_cluster_display_name
#   }
#
#   statement = <<-SQL
# INSERT INTO `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-responses`
# SELECT
#   fp.message_key,
#   fp.fhir_json AS input_medications_json,
#   CAST(AI_TOOL_INVOKE(
#     `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mdm_fhir_conv_model`,
#     CONCAT('Process the medication changes from the following JSON. Extract the "medications_to_add" array and use the add_medications tool for each medication in that array. Pass each medication as a JSON string. The JSON is: ', fp.fhir_json),
#     MAP[],
#     MAP['add_medications', 'Tool to add new medications to the FHIR repository. Takes a MedicationRequest resource as JSON string and creates it in the repository.'],
#     MAP['debug', 'false']
#   ) AS STRING) AS tool_response,
#   CURRENT_TIMESTAMP AS processing_timestamp
# FROM `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.output}` fp
# WHERE fp.fhir_json IS NOT NULL;
# SQL
#
#   depends_on = [
#     confluent_flink_statement.create_fhir_sink_table,
#     confluent_flink_statement.create_mcp_response_sink_table,
#     confluent_flink_statement.mdm_fhir_conv_model_aws,
#     confluent_flink_statement.create_mcp_connection,
#     confluent_flink_statement.create_mcp_tool,
#     confluent_flink_statement.mdm_to_fhir_conversion
#   ]
#
#   lifecycle {
#     ignore_changes = [statement]
#   }
# }

# Drop existing medication processing agent if it exists (to allow recreation)
resource "confluent_flink_statement" "drop_medication_agent" {
  statement_name = "drop-medication-agent"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = "DROP AGENT IF EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`medication_processing_agent`;"

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create agent for processing medications using MCP tools
# Reference: https://docs.confluent.io/cloud/current/flink/reference/statements/create-agent.html
resource "confluent_flink_statement" "create_medication_agent" {
  statement_name = "create-medication-agent"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = <<-SQL
CREATE AGENT `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`medication_processing_agent`
USING MODEL `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mdm_fhir_conv_model`
USING PROMPT 'You are a helpful assistant who uses add_medications and stop_medications tools to respond to user query return only the response that user has asked.'
USING TOOLS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-tool`
COMMENT 'Agent for processing medication changes using add_medications tool to update FHIR repository via MCP'
WITH (
  'max_consecutive_failures' = '3',
  'max_iterations' = '10'
);
SQL

  depends_on = [
    confluent_flink_statement.mdm_fhir_conv_model_aws,
    confluent_flink_statement.create_mcp_tool,
    confluent_flink_statement.drop_medication_agent
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Process medications using the agent: invoke agent to update FHIR repository
# Reference: https://docs.confluent.io/cloud/current/flink/reference/statements/create-agent.html
# Uses AI_RUN_AGENT function to invoke the agent
resource "confluent_flink_statement" "process_medications_with_agent" {
  statement_name = "process-medications-agent"
  organization {
    id = local.flink_common_config.organization.id
  }
  environment {
    id = local.flink_common_config.environment.id
  }
  compute_pool {
    id = local.flink_common_config.compute_pool.id
  }
  principal {
    id = local.flink_common_config.principal.id
  }
  rest_endpoint = local.flink_common_config.rest_endpoint
  credentials {
    key    = local.flink_common_config.credentials.key
    secret = local.flink_common_config.credentials.secret
  }

  properties = {
    "sql.current-catalog"  = local.environment_display_name
    "sql.current-database" = local.kafka_cluster_display_name
  }

  statement = <<-SQL
INSERT INTO `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-responses`
SELECT
  fp.message_key,
  fp.fhir_json AS input_medications_json,
  CAST(agent_result.response AS STRING) AS tool_response,
  CURRENT_TIMESTAMP AS processing_timestamp
FROM `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`${local.kafka_topics.output}` fp,
LATERAL TABLE(AI_RUN_AGENT(
  `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`medication_processing_agent`,
  CONCAT('Use the add_medications and stop_medications tools to process the medication changes from the following JSON: ', fp.fhir_json),
  fp.message_key,
  MAP['debug', 'true']
)) AS agent_result(status, response)
WHERE fp.fhir_json IS NOT NULL;
SQL

  depends_on = [
    confluent_flink_statement.create_fhir_sink_table,
    confluent_flink_statement.create_mcp_response_sink_table,
    confluent_flink_statement.create_medication_agent,
    confluent_flink_statement.mdm_to_fhir_conversion,
    confluent_flink_statement.create_mcp_connection
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}

# Note: This implementation uses Confluent Cloud's native AI model inference capabilities.
# Reference: https://docs.confluent.io/cloud/current/ai/ai-model-inference.html
#
# Key features:
# 1. CREATE CONNECTION: Establishes connection to AWS Bedrock with credentials
# 2. CREATE MODEL: Registers the Bedrock model with Flink SQL
# 3. ML_PREDICT: Native function to invoke the model in SQL queries
#
# AWS Credentials:
# - For production, use IAM roles instead of access keys
# - The connection supports aws_session_token for temporary credentials
# - Ensure the credentials have permissions to invoke the specified Bedrock model
#
# Alternative: If using IAM roles (recommended for production):
# - Remove aws_access_key_id, aws_secret_access_key, and aws_session_token
# - The connection will use the IAM role assigned to the Flink compute pool
# - Ensure the IAM role has bedrock:InvokeModel permissions