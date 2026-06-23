# Infrastructure as code (Terraform)

Provisions the lakehouse object storage — the `lake` bucket with versioning
enabled. The AWS provider is pointed at an S3-compatible endpoint, so the same
config works against MinIO locally, LocalStack, or real AWS by changing
`s3_endpoint` and credentials.

```bash
cd infra/terraform
terraform init
terraform validate
terraform apply        # against MinIO (docker compose up -d) or LocalStack
```

CI runs `terraform init -backend=false && terraform validate` so the config stays
honest on every push. Copy `terraform.tfvars.example` to `terraform.tfvars` to
override defaults.
