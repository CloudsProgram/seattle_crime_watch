terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google={
        source = "hashicorp/google"
    }
  }
}

provider "google" {
    project = var.project
    region = var.region
}

#Data Lake Bucket
resource "google_storage_bucket" "data_lake_bucket" {
    name = "${local.data_lake_bucket}_${var.project}"
    location = var.region

    storage_class = var.storage_class
    uniform_bucket_level_access = true

    versioning {
      enabled = true
    }

    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
        age = 30 // days
      }
    }

    force_destroy = true
}

# Data Warehouse
resource "google_bigquery_dataset" "dataset" {
    count = 2
    dataset_id = var.BQ_DATASET[count.index]
    project = var.project
    location = var.region
    delete_contents_on_destroy = true
  
}