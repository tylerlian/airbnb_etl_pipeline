locals {
  data_lake_bucket = "airbnb_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "healthy-battery-412201"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "airbnb_data"
}

variable "credentials" {
  type    = string
  default = "~/terraform/credentials/my-creds.json"
}