
terraform {
    required_version = ">=1.0"
    backend "local" {}
    required_providers {
        google = {
            source = "hashicorp/google"
        }
    }
}

provider "google" {
    credentials = file(var.provider_c)

    project = var.project_id
    region = var.region
}

# GCS storage bucket
resource "google_storage_bucket" "gtfs-bus" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  # Optional, but recommended settings
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

}

# Artifact registry for containers
resource "google_artifact_registry_repository" "subway-mbta-registry" {
  location      = var.region
  repository_id = var.registry_id
  format        = "DOCKER"
}