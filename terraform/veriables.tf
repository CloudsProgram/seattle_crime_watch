locals {
  data_lake_bucket = "seattle_crime_data_lake"
}

variable "project" {
    description = "Your GCP pltr project id"
}

variable "region" {
    description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
    default = "us-west1"
    type = string
}

variable "storage_class" {
    description = "Storage class type for my bucket"
    default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset, will write data from GCS into BigQuery"
  type = list
  default = ["seattle_crime_staging","seattle_crime"]
}