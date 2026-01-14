# Real-time Clinical Data Extraction (HL7 MDM -> Bedrock -> Flink Agent + MCP)

Terraform-based demo that provisions Confluent Cloud infrastructure and deploys Confluent Flink SQL statements to:

- Ingest **HL7 MDM EDI** messages from a Kafka topic (`hl7-mdm-messages`)
- Use **AWS Bedrock** (via `ML_PREDICT`) to extract medication changes into JSON
- Write extracted JSON to `fhir-processed`
- Use a **Flink Streaming Agent** (`AI_RUN_AGENT`) with an **MCP tool** to call an MCP server (Bedrock AgentCore runtime) and emit results to `mcp-responses`

For a detailed flow, see `DATA_FLOW_DIAGRAM.md`.

## Overview

This project provisions:

- **Confluent Cloud**: Environment, Kafka cluster, topics, service account + API keys, ACLs, Flink compute pool
- **AWS**: A dedicated **IAM user + access keys** for Bedrock invocation (used by the Flink Bedrock connection)
- **Flink SQL (via Terraform)**:
  - Bedrock connection + model (`mdm_fhir_conv_model`)
  - Kafka-catalog tables for source/sinks
  - Statements:
    - `mdm-to-fhir-conversion` (HL7 MDM -> medication JSON extraction)
    - `process-medications-agent` (agent invokes MCP tools; results to `mcp-responses`)

## Prerequisites

- Terraform >= 1.0
- Confluent Cloud account (Kafka + Flink)
- Confluent Cloud API key/secret (Cloud Admin or Org Admin recommended for setup)
- Kafka API key/secret (for topic + ACL operations)
- AWS account with **Bedrock model access enabled** in your chosen region
- AWS credentials locally (only needed for Terraform to create the IAM user/policy in `terraform/infrastructure`)

Optional:
- Schema Registry / Stream Governance (this repo includes optional schema registration controlled by `enable_schema_registry`)

## Project Structure

```
demos/rt-clinical-data-extraction/
├── README.md
├── DATA_FLOW_DIAGRAM.md
└── terraform/
    ├── infrastructure/        # Confluent env/cluster/topics/ACLs/compute pool + AWS IAM user for Bedrock
    └── flink-statements/      # Flink SQL: Bedrock connection/model, tables, agent, MCP connection/tool
```

## Quick Start

This demo uses **two separate Terraform configurations**:

1) `terraform/infrastructure` (base infra)
2) `terraform/flink-statements` (Flink SQL objects; reads infra outputs via remote state)

### 1) Deploy Infrastructure

```bash
cd terraform/infrastructure
terraform init
terraform plan
terraform apply
```

This creates (or reuses, if you provide existing IDs):
- Confluent environment + Kafka cluster
- Topics:
  - `hl7-mdm-messages` (HL7 MDM EDI input)
  - `fhir-processed` (Bedrock extraction output)
- Service account + API keys
- ACLs
- Flink compute pool
- AWS IAM user + access keys for Bedrock invocation

### 2) Deploy Flink Statements (Bedrock + MCP + Agent)

```bash
cd ../flink-statements
terraform init
terraform plan
terraform apply
```

Important:
- This configuration reads remote state from `../infrastructure/terraform.tfstate`, so **run it from this folder** after infra is applied.

## Required Configuration (tfvars)

Each folder has its own `variables.tf` and expects its own `terraform.tfvars`.

### `terraform/infrastructure/terraform.tfvars`

You'll typically set:
- `confluent_cloud_api_key`
- `confluent_cloud_api_secret`
- `confluent_kafka_api_key`
- `confluent_kafka_api_secret`
- Optional: `confluent_environment_id`, `confluent_kafka_cluster_id` (to reuse existing)

### `terraform/flink-statements/terraform.tfvars`

You'll typically set:
- `confluent_cloud_api_key`
- `confluent_cloud_api_secret`

And you **must** configure the MCP server OAuth details used by `CREATE CONNECTION ... type = MCP_SERVER`:
- `mcp_server_endpoint`
- `cognito_client_id`
- `cognito_client_secret`

Security note:
- Prefer using environment variables for secrets (Terraform supports `TF_VAR_*`, e.g. `TF_VAR_cognito_client_secret`), and avoid committing secrets to source control.

## Kafka Topics and Schemas

### Topics used by the pipeline

- **Input**: `hl7-mdm-messages`
  - HL7 MDM EDI payload is carried in `raw_edi_payload`
- **Intermediate**: `fhir-processed`
  - Contains `message_key`, `fhir_json`, `processing_timestamp`
  - `fhir_json` is the Bedrock response JSON
- **Output**: `mcp-responses`
  - Contains `message_key`, `input_medications_json`, `tool_response`, `processing_timestamp`

Notes:
- `hl7-mdm-messages` and `fhir-processed` are created in `terraform/infrastructure`.
- `mcp-responses` is created by the Flink statements as a sink table in the Kafka catalog (and maps to a Kafka topic of the same name).

## Sending Test Messages (HL7 MDM Input)

The repo includes a ready-to-paste sample payload:

- `terraform/flink-statements/sample-message.txt`

You can produce that message to the `hl7-mdm-messages` topic using the Confluent Cloud Console.

Tip:
- Depending on the serializer/schema settings in the Console, you may see "union wrapper" JSON like `{"string": "value"}` (as in the sample). Use the sample as-is if that's what your topic expects.

## What the Flink Statements Do

Deployed from `terraform/flink-statements/`:

- **Bedrock connection**: `bedrock-connection`
  - Uses the dedicated IAM user access keys output by the infrastructure stack
- **LLM model**: `mdm_fhir_conv_model`
  - Provider: Bedrock, task: text generation
  - Used by `ML_PREDICT` to extract medication changes
- **MCP connection + tool**:
  - Connection: `agentcore-mcp-server-connection` (type `MCP_SERVER`)
  - Tool: `mcp-tool` (allowed tools include `add_medications`, `stop_medications`, `greet_user`)
- **Statements**:
  - `mdm-to-fhir-conversion`: reads HL7 MDM EDI, invokes `ML_PREDICT`, writes JSON to `fhir-processed`
  - `process-medications-agent`: reads `fhir-processed`, calls `AI_RUN_AGENT` with `medication_processing_agent`, writes results to `mcp-responses`

## Output JSON Shape (from Bedrock)

`fhir-processed.fhir_json` is expected to be a JSON object with two arrays:

```json
{
  "medications_to_add": [
    {
      "resourceType": "MedicationRequest",
      "status": "active"
    }
  ],
  "medications_to_stop": [
    {
      "resourceType": "MedicationRequest",
      "status": "stopped"
    }
  ]
}
```

Exact fields depend on the model response; the pipeline expects valid JSON and uses the agent/tools to action the changes.

## Cleanup

Destroy in reverse order:

```bash
cd terraform/flink-statements
terraform destroy

cd ../infrastructure
terraform destroy
```

Warning: this deletes Confluent resources (Kafka cluster/topics) and associated data.

## Troubleshooting

### Flink statements fail due to MCP auth/endpoint

- Verify `mcp_server_endpoint`, `cognito_client_id`, and `cognito_client_secret` are correct.
- Ensure your Cognito app client is configured for client-credentials flow and the scope matches what your MCP server expects.

### Bedrock invocation errors

- Confirm the Bedrock model is enabled/allowed in your AWS account/region.
- Confirm the IAM user created by Terraform has `bedrock:InvokeModel` permissions for your configured model ARN(s).

### Kafka ACL issues

- Confirm the service account has READ on `hl7-mdm-messages` and WRITE on sink topics.
- Re-apply `terraform/infrastructure` if ACLs drifted.

## References

- [Confluent Quickstart Streaming Agents](https://github.com/confluentinc/quickstart-streaming-agents/tree/master/aws)
- [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Confluent Flink Docs](https://docs.confluent.io/cloud/current/flink/index.html)
- [AWS Bedrock Docs](https://docs.aws.amazon.com/bedrock/)

## License

Provided as-is for demonstration purposes.
