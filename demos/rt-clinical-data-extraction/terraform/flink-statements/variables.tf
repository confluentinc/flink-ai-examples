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

variable "confluent_cloud_provider" {
  description = "Cloud provider for Confluent resources (AWS, GCP, AZURE)"
  type        = string
  default     = "AWS"
}

variable "confluent_flink_region" {
  description = "Confluent Flink region"
  type        = string
  default     = "us-east-1"
}

# AWS Configuration
variable "aws_region" {
  description = "AWS region for Bedrock"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS Access Key ID for Bedrock connection (optional - if not provided, uses dedicated IAM user from infrastructure). Can be set via TF_VAR_aws_access_key_id environment variable."
  type        = string
  default     = null
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS Secret Access Key for Bedrock connection (optional - if not provided, uses dedicated IAM user from infrastructure). Can be set via TF_VAR_aws_secret_access_key environment variable."
  type        = string
  default     = null
  sensitive   = true
}

variable "aws_session_token" {
  description = "AWS Session Token for temporary credentials (optional - not used with dedicated IAM user). Can be set via TF_VAR_aws_session_token environment variable."
  type        = string
  default     = null
  sensitive   = true
}

# AWS Bedrock Configuration
variable "bedrock_model_id" {
  description = "AWS Bedrock model ID"
  type        = string
  default     = "anthropic.claude-3-5-sonnet-20241022-v2:0"
}

# MCP Server Configuration
variable "mcp_server_endpoint" {
  description = "MCP Server endpoint URL (e.g., https://<agent-id>.bedrock-agentcore.<region>.amazonaws.com/mcp)"
  type        = string
  default     = "https://bedrock-agentcore.us-west-2.amazonaws.com/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aus-west-2%3A829250931565%3Aruntime%2Fmy_mcp_server-FbSsMzBO5S/invocations"
}
# Cognito OAuth Configuration for MCP Server
variable "cognito_user_pool_id" {
  description = "AWS Cognito User Pool ID for OAuth authentication"
  type        = string
  sensitive   = false
  default     = "us-west-2_TMjxPBlCF"
}

variable "cognito_client_id" {
  description = "AWS Cognito App Client ID for OAuth authentication"
  type        = string
  sensitive   = false
  default     = "68r984h7q7ggk03t4vq9k2b0g6"
}

variable "cognito_client_secret" {
  description = "AWS Cognito App Client Secret for OAuth authentication. Can be set via TF_VAR_cognito_client_secret environment variable."
  type        = string
  sensitive   = true
  default     = "31f986hsa2evtsk8jc3e8qctdshjk8ppctjfglc5pe09p4fjss7"
}

variable "cognito_region" {
  description = "AWS region where Cognito User Pool is located"
  type        = string
  default     = "us-west-2"
}

