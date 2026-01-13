# Reference infrastructure outputs using remote state
data "terraform_remote_state" "infrastructure" {
  backend = "local"
  config = {
    path = "../infrastructure/terraform.tfstate"
  }
}

# Confluent Provider
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# Get Flink region data for rest_endpoint
data "confluent_flink_region" "main" {
  cloud  = var.confluent_cloud_provider
  region = var.confluent_flink_region
}

# Common configuration for all Flink statements
locals {
  flink_common_config = {
    organization = {
      id = data.terraform_remote_state.infrastructure.outputs.confluent_organization_id
    }
    environment = {
      id = data.terraform_remote_state.infrastructure.outputs.confluent_environment_id
    }
    compute_pool = {
      id = data.terraform_remote_state.infrastructure.outputs.confluent_flink_compute_pool_id
    }
    principal = {
      id = data.terraform_remote_state.infrastructure.outputs.confluent_service_account_id
    }
    rest_endpoint = data.confluent_flink_region.main.rest_endpoint
    credentials = {
      # Use Flink API key from infrastructure (owned by service account)
      key    = data.terraform_remote_state.infrastructure.outputs.confluent_flink_api_key_id
      secret = data.terraform_remote_state.infrastructure.outputs.confluent_flink_api_key_secret
    }
  }

  # Get display names from infrastructure
  environment_display_name   = data.terraform_remote_state.infrastructure.outputs.confluent_environment_display_name
  kafka_cluster_display_name = data.terraform_remote_state.infrastructure.outputs.confluent_kafka_cluster_display_name

  # Kafka configuration from infrastructure
  kafka_bootstrap_endpoint = data.terraform_remote_state.infrastructure.outputs.confluent_kafka_bootstrap_endpoint
  kafka_api_key_id         = data.terraform_remote_state.infrastructure.outputs.confluent_kafka_api_key_id
  kafka_api_key_secret     = data.terraform_remote_state.infrastructure.outputs.confluent_kafka_api_key_secret

  # Topic names from infrastructure
  # Use try() to handle case where hl7_mdm topic doesn't exist yet in remote state
  kafka_topics = {
    output  = data.terraform_remote_state.infrastructure.outputs.confluent_kafka_topics.output
    hl7_mdm = try(data.terraform_remote_state.infrastructure.outputs.confluent_kafka_topics.hl7_mdm, "hl7-mdm-messages")
  }

  # AWS credentials - Use dedicated IAM user from infrastructure (preferred)
  # Falls back to variables if infrastructure outputs not available
  # This is more secure than using personal AWS credentials
  aws_access_key_id     = try(data.terraform_remote_state.infrastructure.outputs.aws_bedrock_iam_user_access_key_id, var.aws_access_key_id)
  aws_secret_access_key = try(data.terraform_remote_state.infrastructure.outputs.aws_bedrock_iam_user_secret_access_key, var.aws_secret_access_key)
  aws_session_token     = null # IAM user doesn't need session token

  # Schema Registry configuration from infrastructure
  # Use try() to handle case where schema registry outputs don't exist yet in remote state
  schema_registry_rest_endpoint = try(data.terraform_remote_state.infrastructure.outputs.confluent_schema_registry_rest_endpoint, "")
}

