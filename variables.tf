variable "credentials" {
  description = "Credentials to gcp"
  default     = "/Users/sreesanjeev/Desktop/stumpsNDbails/secrets/stumpsndbails-b4fe9300c0a1.json"
}


variable "project" {
  description = "Project"
  default     = "stumpsndbails"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}