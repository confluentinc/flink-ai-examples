# Terraform Configuration for Confluent Cloud

This directory contains Terraform configuration split into **two separate folders** to avoid dependency issues:

1. **`infrastructure/`** - Base infrastructure resources (Environment, Kafka, Flink Compute Pool, topics/ACLs, plus AWS IAM user for Bedrock)
2. **`flink-statements/`** - Flink SQL objects (Bedrock connection + model, tables, agent, MCP connection/tool)

Each folder is a separate Terraform configuration with its own state file.

## Prerequisites

1. **Confluent Cloud Account**: Sign up at [Confluent Cloud](https://confluent.cloud)
2. **Confluent Cloud API Key**: Create an API key with appropriate permissions
   - Go to: `https://confluent.cloud/settings/api-keys`
   - Create a new API key with "Cloud Admin" or "Organization Admin" role
3. **AWS Account**: With appropriate permissions to create IAM users and policies
4. **Terraform**: Version >= 1.0
5. **AWS CLI**: Configured with appropriate credentials

## Setup

### Step 1: Deploy Infrastructure

1. **Navigate to infrastructure folder**:
   ```bash
   cd infrastructure
   ```

2. **Create `terraform.tfvars`** with your values:
   - `confluent_cloud_api_key`: Your Confluent Cloud API key
   - `confluent_cloud_api_secret`: Your Confluent Cloud API secret
   - `confluent_kafka_api_key`: Kafka API key for topic/ACL operations
   - `confluent_kafka_api_secret`: Kafka API secret
   - Configure other variables as needed

3. **Using Existing Confluent Resources** (Optional):
   - To use an existing Confluent Environment, set `confluent_environment_id` in `terraform.tfvars`
   - To use an existing Kafka Cluster, set `confluent_kafka_cluster_id` in `terraform.tfvars`
   - If these are empty, Terraform will create new resources

4. **Initialize and apply**:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

### Step 2: Deploy Flink Statements

1. **Navigate to flink-statements folder**:
   ```bash
   cd ../flink-statements
   ```

2. **Create `terraform.tfvars`** with your values:
   - `confluent_cloud_api_key`: Your Confluent Cloud API key
   - `confluent_cloud_api_secret`: Your Confluent Cloud API secret
   - `mcp_server_endpoint`: MCP Server endpoint URL
   - `cognito_client_id`: Cognito OAuth client id
   - `cognito_client_secret`: Cognito OAuth client secret

   Notes:
   - The Flink statements stack reads **Flink API key/secret**, **Kafka API key/secret**, and the dedicated **AWS Bedrock IAM user access keys** from the infrastructure stack via remote state.
   - You normally do **not** need to provide `confluent_flink_api_key` / `confluent_flink_api_secret` or AWS access keys for this step.

3. **Initialize and apply**:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

**Note**: The Flink statements configuration uses Terraform remote state to automatically read outputs from the infrastructure configuration, so infrastructure must be deployed first.

## Resources Created

### Infrastructure Folder (`infrastructure/`)

**Confluent Cloud Resources**:
- **Confluent Environment**: Container for all Confluent resources
- **Kafka Cluster**: Basic cluster for message streaming
- **Kafka Topics**:
  - `hl7-mdm-messages` (input topic; HL7 MDM EDI messages)
  - `fhir-processed` (output topic; extracted medication JSON)
- **Service Account**: For Flink application to access Kafka
- **API Keys**:
  - Kafka API key for service account
- **ACLs**: Access control lists for Kafka topics and consumer groups
- **Flink Compute Pool**: Compute resources for Flink applications

**AWS Resources**:
- **IAM User + Access Keys**: Dedicated user used by the Flink Bedrock connection
- **IAM Policy**: Grants permissions to invoke Bedrock models
- **CloudWatch Log Group**: For application logging

### Flink Statements Folder (`flink-statements/`)

**Flink SQL Resources**:
- **Bedrock Connection**: Connection to AWS Bedrock for AI model inference
- **Bedrock Model**: Registered model for use in Flink SQL queries
- **Tables/Statements/Agent**:
  - Source table over `hl7-mdm-messages`
  - Sink table over `fhir-processed`
  - Sink table over `mcp-responses`
  - Agent `medication_processing_agent` and statement `process-medications-agent`
  - MCP connection `agentcore-mcp-server-connection` and tool `mcp-tool`

## Outputs

### Infrastructure Outputs

After deploying infrastructure (`cd infrastructure && terraform apply`):

```bash
# Get environment display name (Flink catalog)
terraform output confluent_environment_display_name

# Get Kafka cluster display name (Flink database)
terraform output confluent_kafka_cluster_display_name

# Get Flink compute pool ID
terraform output confluent_flink_compute_pool_id

# Get Kafka bootstrap endpoint
terraform output -raw confluent_kafka_bootstrap_endpoint

# Get service account ID
terraform output confluent_service_account_id
```

### Flink Statements

The Flink statements configuration automatically reads infrastructure outputs via remote state, so you don't need to manually pass values. The Flink statements will be deployed to the infrastructure created in the first step.

## Variables

Each folder has its own `variables.tf` file. See the respective folders for variable documentation.

### Infrastructure Variables

Key variables in `infrastructure/variables.tf`:
- `confluent_cloud_api_key`: Your Confluent Cloud API key
- `confluent_cloud_api_secret`: Your Confluent Cloud API secret
- `confluent_kafka_api_key`: Kafka API key for topic/ACL operations
- `confluent_kafka_api_secret`: Kafka API secret
- `confluent_kafka_region`: Region for Kafka cluster
- `confluent_flink_region`: Region for Flink compute pool
- `flink_max_cfu`: Maximum Confluent Flink Units

### Flink Statements Variables

Key variables in `flink-statements/variables.tf`:
- `confluent_cloud_api_key`: Your Confluent Cloud API key
- `confluent_cloud_api_secret`: Your Confluent Cloud API secret
- `aws_region`: AWS region for Bedrock
- `bedrock_model_id`: AWS Bedrock model to use
- `mcp_server_endpoint`: MCP Server endpoint URL
- `cognito_client_id`: Cognito OAuth client id
- `cognito_client_secret`: Cognito OAuth client secret

## Cleanup

To destroy all resources, destroy in reverse order:

### 1. Destroy Flink Statements First

```bash
cd flink-statements
terraform destroy
```

### 2. Destroy Infrastructure

```bash
cd ../infrastructure
terraform destroy
```

**Warning**: This will delete all Confluent Cloud resources including the Kafka cluster and all data in topics.

## Directory Structure

```
terraform/
├── infrastructure/                 # Base infrastructure resources
│   ├── main.tf                     # Environment, Kafka, Topics, ACLs, Flink Compute Pool, AWS IAM user
│   ├── variables.tf                # Infrastructure variables
│   ├── outputs.tf                  # Infrastructure outputs (used by flink-statements)
│   ├── versions.tf                 # Provider versions
│   └── README.md                   # Infrastructure documentation
└── flink-statements/               # Flink SQL statements
    ├── main.tf                     # Remote state config and common locals
    ├── flink-hl7-mdm-bedrock.tf    # Bedrock connection + model, tables, agent
    ├── flink-mcp-connection.tf     # MCP connection + tool
    ├── variables.tf                # Flink statements variables
    └── versions.tf                 # Provider versions
```

## Troubleshooting

### Confluent Provider Authentication Issues

If you encounter authentication errors:
1. Verify your API key and secret are correct
2. Ensure the API key has appropriate permissions
3. Check that the API key hasn't expired

### Kafka ACL Issues

If the Flink application can't access Kafka topics:
1. Verify ACLs were created: `terraform state list | grep acl`
2. Check service account has correct permissions
3. Review Confluent Cloud UI for ACL configuration

### Flink Compute Pool Issues

If Flink compute pool creation fails:
1. Verify you have sufficient CFU quota
2. Check region availability
3. Ensure cloud provider matches your account type

## References

- [Confluent Terraform Provider Documentation](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Confluent Flink Documentation](https://docs.confluent.io/cloud/current/flink/index.html)
