variable "region" {
  type    = string
  default = "us-east-1"
}

variable "s3_endpoint" {
  type        = string
  description = "S3 endpoint. MinIO: http://localhost:9000, LocalStack: http://localhost:4566"
  default     = "http://localhost:9000"
}

variable "access_key" {
  type    = string
  default = "minioadmin"
}

variable "secret_key" {
  type      = string
  sensitive = true
  default   = "minioadmin"
}

variable "lake_bucket" {
  type    = string
  default = "lake"
}
