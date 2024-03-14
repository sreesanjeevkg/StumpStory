terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.16.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials =  file(var.credentials)
}

resource "google_compute_address" "staticIPAddress" {
  name   = "static-ip-address"
  region = var.region
}

resource "google_compute_instance" "orchestratorVM" {
  name                      = "orchestrator"
  zone                      = "us-central1-a"
  machine_type              = "e2-highcpu-8"
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