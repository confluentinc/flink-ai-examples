output "confluent_environment_id" {
  description = "Confluent Environment ID"
  value       = local.environment_id
}

output "confluent_environment_display_name" {
  description = "Confluent Environment Display Name (used as Flink catalog)"
  value       = local.environment_display_name
}

output "confluent_kafka_cluster_id" {
  description = "Confluent Kafka Cluster ID"
  value       = local.kafka_cluster_id
}

output "confluent_kafka_cluster_display_name" {
  description = "Confluent Kafka Cluster Display Name (used as Flink database)"
  value       = local.kafka_cluster_display_name
}

output "confluent_kafka_bootstrap_endpoint" {
  description = "Confluent Kafka Bootstrap Endpoint"
  value       = local.kafka_cluster_bootstrap
  sensitive   = true
}

output "confluent_kafka_rest_endpoint" {
  description = "Confluent Kafka REST Endpoint"
  value       = local.kafka_cluster_rest
}

output "confluent_kafka_topics" {
  description = "Created Kafka topics"
  value = {
    output     = confluent_kafka_topic.fhir_processed.topic_name
    hl7_mdm    = confluent_kafka_topic.hl7_mdm_messages.topic_name
  }
}

output "confluent_flink_compute_pool_id" {
  description = "Confluent Flink Compute Pool ID"
  value       = confluent_flink_compute_pool.main.id
}

output "confluent_flink_rest_endpoint" {
  description = "Confluent Flink REST Endpoint"
  value       = data.confluent_flink_region.main.rest_endpoint
}

output "confluent_service_account_id" {
  description = "Confluent Service Account ID for Flink application"
  value       = confluent_service_account.flink_app.id
}

output "confluent_kafka_api_key_id" {
  description = "Confluent Kafka API Key ID for Flink service account"
  value       = confluent_api_key.flink_app_kafka_api_key.id
  sensitive   = false
}

output "confluent_kafka_api_key_secret" {
  description = "Confluent Kafka API Secret for Flink service account"
  value       = confluent_api_key.flink_app_kafka_api_key.secret
  sensitive   = true
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "confluent_organization_id" {
  description = "Confluent Organization ID"
  value       = var.confluent_organization_id
}

output "confluent_flink_api_key_id" {
  description = "Confluent Flink API Key ID for service account"
  value       = confluent_api_key.flink_app_flink_api_key.id
  sensitive   = false
}

output "confluent_flink_api_key_secret" {
  description = "Confluent Flink API Key Secret for service account"
  value       = confluent_api_key.flink_app_flink_api_key.secret
  sensitive   = true
}

output "aws_bedrock_iam_user_access_key_id" {
  description = "AWS IAM User Access Key ID for Bedrock connection (dedicated user, not personal credentials)"
  value       = aws_iam_access_key.flink_bedrock_user.id
  sensitive   = true
}

output "aws_bedrock_iam_user_secret_access_key" {
  description = "AWS IAM User Secret Access Key for Bedrock connection (dedicated user, not personal credentials)"
  value       = aws_iam_access_key.flink_bedrock_user.secret
  sensitive   = true
}

output "aws_bedrock_iam_user_arn" {
  description = "AWS IAM User ARN for Bedrock access"
  value       = aws_iam_user.flink_bedrock_user.arn
}

output "confluent_schema_registry_cluster_id" {
  description = "Confluent Schema Registry Cluster ID"
  value       = var.enable_schema_registry ? var.confluent_schema_registry_cluster_id : null
}

output "confluent_schema_registry_rest_endpoint" {
  description = "Confluent Schema Registry REST Endpoint"
  value       = var.enable_schema_registry ? var.confluent_schema_registry_rest_endpoint : ""
}
