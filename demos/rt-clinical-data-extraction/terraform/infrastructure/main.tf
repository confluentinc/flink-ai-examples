
# Confluent Provider
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# AWS Provider (for Bedrock access)
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "flink-mdm-healthlake"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Confluent Environment - use existing or create new
data "confluent_environment" "main" {
  count = var.confluent_environment_id != "" ? 1 : 0
  id    = var.confluent_environment_id
}

resource "confluent_environment" "main" {
  count        = var.confluent_environment_id != "" ? 0 : 1
  display_name = "${var.project_name}-${var.environment}"
  
  stream_governance {
    package = "ADVANCED"  # Required for Schema Registry
  }
}




locals {
  environment_id           = var.confluent_environment_id != "" ? data.confluent_environment.main[0].id : confluent_environment.main[0].id
  environment_display_name = var.confluent_environment_id != "" ? data.confluent_environment.main[0].display_name : confluent_environment.main[0].display_name
}

# Confluent Kafka Cluster - use existing or create new
data "confluent_kafka_cluster" "main" {
  count = var.confluent_kafka_cluster_id != "" ? 1 : 0
  id    = var.confluent_kafka_cluster_id
  environment {
    id = local.environment_id
  }
}

resource "confluent_kafka_cluster" "main" {
  count        = var.confluent_kafka_cluster_id != "" ? 0 : 1
  display_name = "${var.project_name}-${var.environment}"
  availability = var.confluent_kafka_availability
  cloud        = var.confluent_cloud_provider
  region       = var.confluent_kafka_region
  basic {}

  environment {
    id = local.environment_id
  }

  lifecycle {
    prevent_destroy = false
  }
}

locals {
  kafka_cluster_id           = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].id : confluent_kafka_cluster.main[0].id
  kafka_cluster_bootstrap    = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].bootstrap_endpoint : confluent_kafka_cluster.main[0].bootstrap_endpoint
  kafka_cluster_rest         = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].rest_endpoint : confluent_kafka_cluster.main[0].rest_endpoint
  kafka_cluster_api_version  = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].api_version : confluent_kafka_cluster.main[0].api_version
  kafka_cluster_kind         = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].kind : confluent_kafka_cluster.main[0].kind
  kafka_cluster_display_name = var.confluent_kafka_cluster_id != "" ? data.confluent_kafka_cluster.main[0].display_name : confluent_kafka_cluster.main[0].display_name
}

# Confluent Service Account for Flink Application
resource "confluent_service_account" "flink_app" {
  display_name = "${var.project_name}-flink-app-${var.environment}"
  description  = "Service account for Flink application to access Kafka and Flink compute pool"
}

# Role binding for service account to manage Flink resources
# Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/core/main.tf
# This grants the service account EnvironmentAdmin role to create Flink connections and models
resource "confluent_role_binding" "flink_app_environment_admin" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = var.confluent_environment_id != "" ? data.confluent_environment.main[0].resource_name : confluent_environment.main[0].resource_name
}

# Confluent API Key for Flink Service Account
resource "confluent_api_key" "flink_app_kafka_api_key" {
  display_name = "${var.project_name}-flink-kafka-api-key-${var.environment}"
  description  = "Kafka API Key for Flink application service account"

  owner {
    id          = confluent_service_account.flink_app.id
    api_version = confluent_service_account.flink_app.api_version
    kind        = confluent_service_account.flink_app.kind
  }

  managed_resource {
    id          = local.kafka_cluster_id
    api_version = local.kafka_cluster_api_version
    kind        = local.kafka_cluster_kind

    environment {
      id = local.environment_id
    }
  }
}

# Kafka Topics
# Using dedicated Kafka API key for topic creation
resource "confluent_kafka_topic" "fhir_processed" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name    = var.kafka_output_topic
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }

  partitions_count = var.kafka_topic_partitions
  config = {
    "retention.ms" = var.kafka_topic_retention_ms
  }
}

# Kafka Topic for HL7 MDM (Medical Document Management) EDI Messages
# Note: MDM messages are EDI format (text/pipe-delimited), not FHIR (JSON)
resource "confluent_kafka_topic" "hl7_mdm_messages" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  topic_name    = var.kafka_hl7_mdm_topic
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }

  partitions_count = var.kafka_topic_partitions
  config = {
    "retention.ms" = var.kafka_topic_retention_ms
  }
}


# ACLs for Flink Service Account
# Using dedicated Kafka API key for ACL creation (needs ALTER permission)
resource "confluent_kafka_acl" "flink_app_producer" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.fhir_processed.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_app.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }
}

resource "confluent_kafka_acl" "flink_app_consumer_group" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  resource_type = "GROUP"
  resource_name = var.kafka_consumer_group_id
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }
}

# ACLs for HL7 MDM Topic
resource "confluent_kafka_acl" "flink_app_hl7_mdm_consumer" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.hl7_mdm_messages.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }
}

resource "confluent_kafka_acl" "flink_app_hl7_mdm_producer" {
  kafka_cluster {
    id = local.kafka_cluster_id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.hl7_mdm_messages.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_app.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = local.kafka_cluster_rest
  credentials {
    key    = var.confluent_kafka_api_key
    secret = var.confluent_kafka_api_secret
  }
}

# Confluent Flink Compute Pool
resource "confluent_flink_compute_pool" "main" {
  display_name = "${var.project_name}-${var.environment}"
  cloud        = var.confluent_cloud_provider
  region       = var.confluent_flink_region
  max_cfu      = var.flink_max_cfu
  environment {
    id = local.environment_id
  }
}

# Get Flink region data for rest_endpoint
data "confluent_flink_region" "main" {
  cloud  = var.confluent_cloud_provider
  region = var.confluent_flink_region
}

# Flink API Key for service account (scoped to Flink region)
# Reference: https://github.com/confluentinc/quickstart-streaming-agents/blob/master/aws/core/main.tf
resource "confluent_api_key" "flink_app_flink_api_key" {
  display_name = "${var.project_name}-flink-api-key-${var.environment}"
  description  = "Flink API Key that is owned by 'flink_app' service account"
  owner {
    id          = confluent_service_account.flink_app.id
    api_version = confluent_service_account.flink_app.api_version
    kind        = confluent_service_account.flink_app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = local.environment_id
    }
  }

  depends_on = [
    confluent_role_binding.flink_app_environment_admin
  ]
}

# Dedicated IAM User for Bedrock Access (replaces IAM role)
# This is more secure than using personal AWS credentials
resource "aws_iam_user" "flink_bedrock_user" {
  name = "${var.project_name}-${var.environment}-bedrock-user"
  path = "/confluent-flink/"

  tags = {
    Name        = "Flink Bedrock User"
    Purpose     = "Bedrock model invocation from Confluent Flink"
    ManagedBy   = "terraform"
    Project     = var.project_name
    Environment = var.environment
  }
}

# IAM Policy for Bedrock Access (minimal permissions)
resource "aws_iam_user_policy" "flink_bedrock_user_policy" {
  name = "${var.project_name}-${var.environment}-bedrock-policy"
  user = aws_iam_user.flink_bedrock_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream"
        ]
        Resource = var.bedrock_model_arns
      }
    ]
  })
}

# Access keys for the IAM user
# These will be used by the Bedrock connection
resource "aws_iam_access_key" "flink_bedrock_user" {
  user = aws_iam_user.flink_bedrock_user.name
}

# CloudWatch Log Group for Flink (if needed for monitoring)
resource "aws_cloudwatch_log_group" "flink" {
  name              = "/confluent/flink/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days
}

# ============================================================================
# Schema Registry Resources (created after all other resources)
# ============================================================================

# JSON Schema definition for HL7 MDM messages
locals {
  edi_json_schema = jsonencode({
    type = "object"
    properties = {
      edi_type = {
        type = "string"
      }
      edi_timestamp = {
        type = "string"
        format = "date-time"
      }
      raw_edi_payload = {
        type = "string"
      }
    }
    required = ["edi_type", "raw_edi_payload"]
  })
}

# Register JSON Schema for HL7 MDM topic in Schema Registry
# Schema subject name follows the default topic-value naming strategy
# This is created after all other resources to ensure proper dependency ordering
# Note: Schema Registry requires Stream Governance to be enabled on the environment
# Provide confluent_schema_registry_cluster_id and confluent_schema_registry_rest_endpoint in terraform.tfvars
#
# IMPORTANT: If you get a schema compatibility error, it means a schema with the same subject name already exists.
# You have two options:
# 1. Delete the existing schema from Schema Registry Console (recommended for development)
# 2. Change the subject name by modifying the subject_name below
resource "confluent_schema" "hl7_mdm_schema" {
  count = var.enable_schema_registry ? 1 : 0
  
  schema_registry_cluster {
    id = var.confluent_schema_registry_cluster_id 
  }
  rest_endpoint = var.confluent_schema_registry_rest_endpoint 
  credentials {
    key    = var.confluent_schema_registry_api_key
    secret = var.confluent_schema_registry_api_secret
  }
  
  # Subject name follows the default topic-value naming strategy
  subject_name = "${confluent_kafka_topic.hl7_mdm_messages.topic_name}-value"
  schema       = local.edi_json_schema
  format       = "JSON"

  depends_on = [
    confluent_kafka_topic.hl7_mdm_messages
  ]

  lifecycle {
    # Allow replacement if schema changes
    create_before_destroy = true
    # Ignore changes to schema content to avoid compatibility issues
    # Uncomment the line below if you want Terraform to ignore schema changes
    # ignore_changes = [schema]
  }
}

