# Create MCP Server connection for FHIR operations using Flink SQL
# Based on Confluent quickstart pattern: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/lab1-tool-calling/main.tf
# The MCP server provides FHIR resources (Patient, Observation, etc.) as tools
# Endpoint format: https://bedrock-agentcore.<region>.amazonaws.com/runtimes/<runtime-arn>/invocations
# Reference: https://docs.confluent.io/cloud/current/flink/reference/statements/create-connection.html
resource "confluent_flink_statement" "create_mcp_connection" {
  statement_name = "create-mcp-connection"
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

  # CREATE CONNECTION statement following Confluent quickstart pattern
  # Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/lab1-tool-calling/main.tf
  # Uses 'MCP_SERVER' type (uppercase) and OAuth with Cognito for authentication
  # Confluent Cloud extracts secrets to the secret store, ensuring they aren't displayed
  # in subsequent DESCRIBE CONNECTION statements or the Flink SQL shell
  # Cognito token endpoint format: https://cognito-idp.{region}.amazonaws.com/{userPoolId}/oauth2/token
  # Using OAuth with confidential client - Confluent Flink expects parameter names without 'oauth.' prefix
  # For Cognito Client Credentials flow, scope is required by Confluent but may need to be empty for Cognito
  # Note: Confluent Flink does not support 'audience' or 'grant-type' parameters for MCP_SERVER connections
  # Confluent Flink automatically infers grant_type=client_credentials when client-id and client-secret are provided
  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`agentcore-mcp-server-connection`
    WITH (
      'type' = 'MCP_SERVER',
      'endpoint' = '${var.mcp_server_endpoint}',
      'client-id' = '${var.cognito_client_id}',
      'client-secret' = '${var.cognito_client_secret}',
      'token-endpoint' = 'https://us-west-2tmjxpblcf.auth.us-west-2.amazoncognito.com/oauth2/token',
      'scope' = 'bedrock-agentcore-runtime/mcp.invoke',
      'transport-type' = 'STREAMABLE_HTTP'
    );
  EOT

  lifecycle {
    ignore_changes = [statement]
  }
}

# Create a TOOL using the MCP server connection
# Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/lab1-tool-calling/main.tf
# The tool allows invoking MCP server tools through the connection
resource "confluent_flink_statement" "create_mcp_tool" {
  statement_name = "create-mcp-tool"
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

  # CREATE TOOL statement using the MCP server connection
  # Reference: https://docs.confluent.io/cloud/current/ai/streaming-agents/call-tools.html#streaming-agents-call-tools
  # Reference: https://docs.confluent.io/cloud/current/flink/reference/statements/create-tool.html
  # Using fully qualified names for consistency
  # Allowed options: REQUEST_TIMEOUT, MAX_RETRIES, ALLOWED_TOOLS, TYPE
  # The 'allowed_tools' parameter specifies which tools from the MCP server can be invoked
  statement = <<-SQL
CREATE TOOL IF NOT EXISTS `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`mcp-tool`
USING CONNECTION `${local.environment_display_name}`.`${local.kafka_cluster_display_name}`.`agentcore-mcp-server-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'greet_user,add_medications,stop_medications'
);
SQL

  depends_on = [
    confluent_flink_statement.create_mcp_connection
  ]

  lifecycle {
    ignore_changes = [statement]
  }
}


