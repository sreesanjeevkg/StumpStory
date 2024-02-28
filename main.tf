terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.16.0"
    }
  }
}

provider "google" {
  project     = "stumpsndbails"
  region      = "us-central1"
  zone        = "us-central1-a"
  credentials = "/Users/sreesanjeev/Desktop/stumpsNDbails/secrets/stumpsndbails-b4fe9300c0a1.json"
}

resource "google_compute_address" "staticIPAddress" {
  name   = "static-ip-address"
  region = "us-central1"
}

resource "google_compute_instance" "orchestratorVM" {
  name                      = "orchestrator"
  zone                      = "us-central1-a"
  machine_type              = "e2-standard-2"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-2004-lts"
      size  = 50
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.staticIPAddress.address
    }
  }
}

resource "google_storage_bucket" "storageBucket" {
  name     = "stumpsndbails"
  location = "US"
  versioning {
    enabled = false
  }
}