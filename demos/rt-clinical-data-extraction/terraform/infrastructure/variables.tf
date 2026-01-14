# Confluent Cloud Configuration
variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "confluent_kafka_api_key" {
  description = "Confluent Kafka API Key for topic and ACL operations"
  type        = string
  sensitive   = true
}

variable "confluent_kafka_api_secret" {
  description = "Confluent Kafka API Secret for topic and ACL operations"
  type        = string
  sensitive   = true
}

variable "confluent_schema_registry_api_key" {
  description = "Confluent Schema Registry API Key for schema operations"
  type        = string
  sensitive   = true
}

variable "confluent_schema_registry_api_secret" {
  description = "Confluent Schema Registry API Secret for schema operations"
  type        = string
  sensitive   = true
}

variable "confluent_flink_api_key" {
  description = "Confluent Flink API Key for Flink statement operations"
  type        = string
  sensitive   = true
}

variable "confluent_flink_api_secret" {
  description = "Confluent Flink API Secret for Flink statement operations"
  type        = string
  sensitive   = true
}

variable "confluent_organization_id" {
  description = "Confluent Organization ID (found in Confluent Cloud UI under Settings > Organization Settings)"
  type        = string
}

variable "confluent_environment_id" {
  description = "Existing Confluent Environment ID to use (leave empty to create new)"
  type        = string
  default     = ""
}

variable "confluent_kafka_cluster_id" {
  description = "Existing Confluent Kafka Cluster ID to use (leave empty to create new)"
  type        = string
  default     = ""
}

variable "confluent_schema_registry_cluster_id" {
  description = "Confluent Schema Registry Cluster ID (required if enable_schema_registry is true)"
  type        = string
  default     = ""
}

variable "confluent_schema_registry_rest_endpoint" {
  description = "Confluent Schema Registry REST Endpoint (required if enable_schema_registry is true)"
  type        = string
  default     = ""
}

variable "confluent_cloud_provider" {
  description = "Cloud provider for Confluent resources (AWS, GCP, AZURE) - only used when creating new cluster"
  type        = string
  default     = "AWS"
}

variable "confluent_kafka_availability" {
  description = "Kafka cluster availability (SINGLE_ZONE, MULTI_ZONE) - only used when creating new cluster"
  type        = string
  default     = "SINGLE_ZONE"
}

variable "confluent_kafka_region" {
  description = "Confluent Kafka region - only used when creating new cluster"
  type        = string
  default     = "us-east-1"
}

variable "confluent_flink_region" {
  description = "Confluent Flink region"
  type        = string
  default     = "us-east-1"
}

# Project Configuration
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "flink-mdm-healthlake"
}

# Kafka Configuration
variable "kafka_input_topic" {
  description = "DEPRECATED (unused): previously the Kafka input topic for FHIR messages. Use kafka_hl7_mdm_topic for HL7 MDM EDI input instead."
  type        = string
  default     = "hl7-mdm-messages"
}

variable "kafka_output_topic" {
  description = "Kafka output topic name for processed messages"
  type        = string
  default     = "fhir-processed"
}

variable "kafka_hl7_mdm_topic" {
  description = "Kafka topic name for HL7 MDM (Medical Document Management) EDI messages"
  type        = string
  default     = "hl7-mdm-messages"
}

variable "enable_schema_registry" {
  description = "Enable Schema Registry integration. Requires Stream Governance (ADVANCED package) to be enabled on the environment. Set to false if Stream Governance is not enabled."
  type        = bool
  default     = false
}

variable "kafka_consumer_group_id" {
  description = "Kafka consumer group ID"
  type        = string
  default     = "flink-fhir-consumer"
}

variable "kafka_topic_partitions" {
  description = "Number of partitions for Kafka topics"
  type        = number
  default     = 3
}

variable "kafka_topic_retention_ms" {
  description = "Kafka topic retention in milliseconds"
  type        = string
  default     = "604800000" # 7 days
}

# Flink Configuration
variable "flink_max_cfu" {
  description = "Maximum CFU (Confluent Flink Units) for compute pool"
  type        = number
  default     = 5
}

variable "flink_parallelism" {
  description = "Flink application parallelism"
  type        = number
  default     = 1
}

variable "flink_checkpoint_interval" {
  description = "Flink checkpoint interval in milliseconds"
  type        = number
  default     = 60000
}

# AWS Configuration
variable "aws_region" {
  description = "AWS region for Bedrock and IAM resources"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS Access Key ID for Bedrock connection (optional if using IAM roles)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS Secret Access Key for Bedrock connection (optional if using IAM roles)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "aws_session_token" {
  description = "AWS Session Token for temporary credentials (optional)"
  type        = string
  default     = ""
  sensitive   = true
}

# AWS Bedrock Configuration
variable "bedrock_model_id" {
  description = "AWS Bedrock model ID"
  type        = string
  default     = "anthropic.claude-3-5-sonnet-20241022-v2:0"
}

variable "bedrock_model_arns" {
  description = "List of Bedrock model ARNs for IAM policy"
  type        = list(string)
  default = [
    "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"
  ]
}

# Logging
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}
