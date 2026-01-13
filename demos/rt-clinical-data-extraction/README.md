# Flink MDM HealthLake Streaming Agent

A Terraform-based infrastructure project for deploying a Flink streaming application that consumes HL7 FHIR messages from Confluent Kafka topics and processes them using AWS Bedrock.

## Overview

This project uses Terraform to provision:
1. Confluent Cloud Kafka cluster and topics for HL7 FHIR messages
2. Confluent Flink compute pool for stream processing
3. Flink SQL statements to process FHIR messages and invoke AWS Bedrock
4. AWS IAM roles and policies for Bedrock access

## Prerequisites

- Terraform >= 1.0
- Confluent Cloud account (for Kafka and Flink)
- AWS Account with Bedrock access
- Confluent Cloud API Key with appropriate permissions
- AWS CLI configured (for IAM resource creation)

## Project Structure

```
flink-mdm-healthlake/
├── README.md                                  # This file
├── terraform/                                 # Terraform infrastructure code
│   ├── infrastructure/                       # Base infrastructure resources
│   │   ├── main.tf                            # Environment, Kafka, Topics, ACLs, Flink Compute Pool, AWS IAM
│   │   ├── variables.tf                       # Infrastructure variables
│   │   ├── outputs.tf                         # Infrastructure outputs
│   │   ├── versions.tf                        # Provider versions
│   │   └── README.md                          # Infrastructure documentation
│   │
│   └── flink-statements/                      # Flink SQL statements
│       ├── main.tf                            # Remote state config and common locals
│       ├── flink-hl7-mdm-bedrock.tf           # Bedrock connection and model statements
│       ├── variables.tf                       # Flink statements variables
│       ├── versions.tf                        # Provider versions
│       └── README.md                          # Flink statements documentation
```

## Quick Start

This project uses **two separate Terraform configurations** to avoid dependency issues:
1. **Infrastructure** - Base resources (Environment, Kafka, Flink Compute Pool, etc.)
2. **Flink Statements** - Flink SQL statements (Bedrock connection and model)

### 1. Deploy Infrastructure First

```bash
cd terraform/infrastructure

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply the configuration
terraform apply

# Get outputs (Kafka endpoints, API keys, etc.)
terraform output
```

This creates:
- Confluent Environment
- Kafka Cluster and Topics
- Service Account and API Keys
- ACLs
- Flink Compute Pool
- AWS IAM Role for Bedrock

### 2. Deploy Flink Statements

After infrastructure is deployed, deploy the Flink SQL statements:

```bash
cd ../flink-statements

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply the configuration
terraform apply
```

This creates:
- AWS Bedrock Connection
- Bedrock Model for FHIR analysis

**Note**: The Flink statements configuration uses Terraform remote state to reference infrastructure outputs, so infrastructure must be deployed first.

### 3. Configure Variables

Each folder has its own `variables.tf`. You can create `terraform.tfvars` files in each folder with your credentials:

**infrastructure/terraform.tfvars**:
- `confluent_cloud_api_key`: Your Confluent Cloud API key
- `confluent_cloud_api_secret`: Your Confluent Cloud API secret
- `confluent_kafka_api_key`: Kafka API key for topic/ACL operations
- `confluent_kafka_api_secret`: Kafka API secret
- Configure other variables as needed

**flink-statements/terraform.tfvars**:
- `confluent_cloud_api_key`: Your Confluent Cloud API key
- `confluent_cloud_api_secret`: Your Confluent Cloud API secret
- `confluent_flink_api_key`: Flink API key for statement operations
- `confluent_flink_api_secret`: Flink API secret
- AWS credentials (optional if using IAM roles)

## Infrastructure Components

### Confluent Cloud Resources

- **Environment**: Container for all Confluent resources
- **Kafka Cluster**: Basic cluster for message streaming
- **Kafka Topics**: 
  - `fhir-messages` (input topic)
  - `fhir-processed` (output topic)
- **Service Account**: For Flink application to access Kafka
- **API Keys**: 
  - Kafka API key for service account
  - Flink API key for service account
- **ACLs**: Access control lists for Kafka topics and consumer groups
- **Flink Compute Pool**: Compute resources for Flink applications

### AWS Resources

- **IAM Role**: For accessing AWS Bedrock
- **IAM Policy**: Grants permissions to invoke Bedrock models
- **CloudWatch Log Group**: For application logging

## Flink SQL Examples

### Basic FHIR Message Processing

```sql
-- Create source table for FHIR messages
CREATE TABLE fhir_messages (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'fhir-messages',
  'properties.bootstrap.servers' = '<kafka-bootstrap-endpoint>',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";',
  'format' = 'raw',
  'scan.startup.mode' = 'latest-offset'
);

-- Create sink table for processed messages
CREATE TABLE fhir_processed (
  result STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'fhir-processed',
  'properties.bootstrap.servers' = '<kafka-bootstrap-endpoint>',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";',
  'format' = 'raw'
);

-- Process messages (add your transformation logic here)
INSERT INTO fhir_processed
SELECT message FROM fhir_messages;
```

### Using Flink UDFs for Bedrock Integration

For AWS Bedrock integration, you'll need to create custom Flink UDFs (User Defined Functions) or use Flink's HTTP connector. See `terraform/flink-application.tf.example` for more examples.

## Configuration

### Terraform Variables

Key variables in `terraform/variables.tf`:

- `confluent_cloud_api_key`: Confluent Cloud API key
- `confluent_cloud_api_secret`: Confluent Cloud API secret
- `confluent_kafka_region`: Region for Kafka cluster
- `confluent_flink_region`: Region for Flink compute pool
- `kafka_input_topic`: Input topic name (default: `fhir-messages`)
- `kafka_output_topic`: Output topic name (default: `fhir-processed`)
- `flink_max_cfu`: Maximum Confluent Flink Units
- `bedrock_model_id`: AWS Bedrock model ID
- `aws_region`: AWS region for Bedrock

See `terraform/terraform.tfvars.example` for all available variables.

## AWS Bedrock Setup

1. **Enable Bedrock Access**: Ensure Bedrock is enabled in your AWS account and region
2. **IAM Permissions**: Terraform automatically creates an IAM role with the following permissions:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "bedrock:InvokeModel",
           "bedrock:InvokeModelWithResponseStream"
         ],
         "Resource": "arn:aws:bedrock:*::foundation-model/*"
       }
     ]
   }
   ```
3. **Model Access**: Request access to the specific Bedrock model you want to use in the AWS Console

## Kafka Topics

Topics are automatically created by Terraform:
- **Input Topic**: `hl7-mdm-messages` (HL7 MDM EDI messages)
- **Output Topic**: `fhir-processed` (converted FHIR JSON messages)

To produce test messages, use the Confluent Cloud UI or CLI with the bootstrap endpoint from Terraform outputs.

### Sending Messages via Confluent Cloud Console

You can send test messages directly through the Confluent Cloud Console. The topic uses Avro schema format with union types. Here's a sample message you can use:

```json
{
  "message_key": {
    "string": "oTthQxIIXKBFOYvmmJwIUw"
  },
  "edi_type": {
    "string": "HL7_MDM"
  },
  "edi_timestamp": {
    "string": "2024-01-15T10:30:00Z"
  },
  "raw_edi_payload": {
    "string": "MSH|^~\\&|EPIC|EMORY_HOSP|STREAM_BUS|HCLS_DEMO|202512181530||MDM^T02^MDM_T02|MSGID99812|P|2.5\\rPID|1||PAT12345^^^MRN||DOE^JONATHAN^W||19780520|M||W|123 MAIN ST^^ATLANTA^GA^30303||555-0199|||||\\rPV1|1|I|2N^201^01||||12345^SMITH^ALICE^M^DR||||||||||||ADM10092||||||||||||||||||||||||202512120800|202512181400\\rTXA|1|DS|TX/RTF|202512181525|||||12345^SMITH^ALICE^M^DR|||||||||P\\rOBX|1|TX|DS_SUMMARY^Discharge Summary^L||HOSPITAL COURSE: Patient admitted for acute myocardial infarction. Progressed well post-stent. \\X0D\\DISCHARGE MEDICATIONS: \\X0D\\1. Lisinopril 10mg PO Daily. \\X0D\\2. Atorvastatin 40mg at bedtime. \\X0D\\3. Metoprolol Tartrate 25mg BID. \\X0D\\4. DISCONTINUE pre-admission Amlodipine 5mg. \\X0D\\FOLLOW UP: See cardiology in 1 week.||||||F"
  }
}
```

**Note**: This message format uses Avro union types `["null", "string"]`, where string values are represented as `{"string": "value"}` and null values as `null`. The `raw_edi_payload` contains an HL7 MDM EDI message with escape sequences properly escaped for JSON.

Alternatively, you can use the Python test client (see `README_TEST_CLIENT.md`) which handles the message formatting automatically.

## HL7 FHIR Message Format

The application expects HL7 FHIR R4 JSON format messages. Example:

```json
{
  "resourceType": "Bundle",
  "type": "transaction",
  "entry": [
    {
      "resource": {
        "resourceType": "Patient",
        "id": "example",
        "name": [
          {
            "use": "official",
            "family": "Doe",
            "given": ["John"]
          }
        ]
      }
    }
  ]
}
```

## Terraform Outputs

### Infrastructure Outputs

After deploying infrastructure (`cd terraform/infrastructure && terraform apply`):

- `confluent_environment_id`: Environment ID
- `confluent_environment_display_name`: Environment display name (used as Flink catalog)
- `confluent_kafka_cluster_id`: Kafka cluster ID
- `confluent_kafka_cluster_display_name`: Kafka cluster display name (used as Flink database)
- `confluent_kafka_bootstrap_endpoint`: Kafka bootstrap endpoint
- `confluent_kafka_rest_endpoint`: Kafka REST endpoint
- `confluent_flink_compute_pool_id`: Flink compute pool ID
- `confluent_flink_rest_endpoint`: Flink REST endpoint
- `confluent_service_account_id`: Service account ID
- `confluent_kafka_api_key_id`: Kafka API key ID
- `confluent_kafka_api_key_secret`: Kafka API secret (sensitive)
- `aws_region`: AWS region
- `confluent_organization_id`: Organization ID

### Flink Statements

The Flink statements configuration automatically reads infrastructure outputs via remote state, so you don't need to manually pass values.

## Cleanup

To destroy all resources, destroy in reverse order:

### 1. Destroy Flink Statements First

```bash
cd terraform/flink-statements
terraform destroy
```

### 2. Destroy Infrastructure

```bash
cd ../infrastructure
terraform destroy
```

**Warning**: This will delete all Confluent Cloud resources including the Kafka cluster and all data in topics.

## Troubleshooting

### Confluent Provider Authentication Issues

If you encounter authentication errors:
1. Verify your API key and secret are correct in `terraform.tfvars`
2. Ensure the API key has appropriate permissions (Cloud Admin or Organization Admin)
3. Check that the API key hasn't expired

### Kafka ACL Issues

If the Flink application can't access Kafka topics:
1. Verify ACLs were created: `terraform state list | grep acl`
2. Check service account has correct permissions
3. Review Confluent Cloud UI for ACL configuration

### Flink Compute Pool Issues

If Flink compute pool creation fails:
1. Verify you have sufficient CFU quota in Confluent Cloud
2. Check region availability
3. Ensure cloud provider matches your account type

## References

- [Confluent Quickstart Streaming Agents](https://github.com/confluentinc/quickstart-streaming-agents/tree/master/aws)
- [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Confluent Flink Documentation](https://docs.confluent.io/cloud/current/flink/index.html)
- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/)
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/)

## License

This project is provided as-is for demonstration purposes.
