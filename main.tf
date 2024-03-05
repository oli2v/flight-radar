terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("flight-radar-service-account-credentials.json")

  project = "flight-radar-415911"
  region  = "europe-west9"
  zone    = "europe-west9"
}

resource "google_storage_bucket" "default" {
  name          = "flight-radar-bucket"
  location      = "EUROPE-WEST9"
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "default" {
  dataset_id = "flight_radar_dataset"
  location   = "EUROPE-WEST9"

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "flights"

  labels = {
    env = "default"
  }

  schema = file("flights_schema.json")

}
