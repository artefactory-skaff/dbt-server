terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.81.0"
    }
  }
}

provider "google" {
  project     = "stc-dbt-test-9e19"
}

variable "project_id" {
  type = string
  default = "stc-dbt-test-9e19"
}

variable "service_account" {
  type = string
  default = "stc-dbt-terraform-sa@stc-dbt-test-9e19.iam.gserviceaccount.com"
}

variable "docker_image" {
  type = string
  default = "us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image"
}

variable "bucket_name" {
  type = string
  default = "dbt-stc-test2"
}



resource "google_storage_bucket" "static" {
  name          = var.bucket_name
  location      = "EU"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}

resource "google_project_service" "firestore" {
  project = var.project_id
  service = "firestore.googleapis.com"
}

# not necessary:
resource "google_firestore_document" "first_status" {
  project     = var.project_id
  collection  = "dbt-status"
  document_id = "0"
  fields      = "{\"cloud_storage_folder\":{\"stringValue\":\"2023-09-08-0\"},\"log_starting_byte\":{\"stringValue\":\"0\"},\"run_status\":{\"stringValue\":\"started\"},\"user_command\":{\"stringValue\":\"\"},\"uuid\":{\"stringValue\":\"0\"}}"
}



resource "google_project_service" "run_api" {
  service = "run.googleapis.com"

  disable_on_destroy = true
}


# dbt-server dev on Cloud Run
resource "google_cloud_run_service" "server_dev" {
  name = "server-dev-tf"
  location = "europe-west9"

  template {
    spec {
      containers {
        image = "${var.docker_image}:dev"
        env {
          name = "BUCKET_NAME"
          value = var.bucket_name
        }
        env {
          name = "DOCKER_IMAGE"
          value = "${var.docker_image}:dev"
        }
        env {
          name = "SERVICE_ACCOUNT"
          value = var.service_account
        }
        env {
          name = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "LOCATION"
          value = "europe-west9"
        }
      }
    } 
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run_api]
}

resource "google_cloud_run_service_iam_member" "run_all_users_dev" {
  service  = google_cloud_run_service.server_dev.name
  location = google_cloud_run_service.server_dev.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}



# dbt-server prod on Cloud Run
resource "google_cloud_run_service" "server_prod" {
  name = "server-prod-tf"
  location = "europe-west9"

  template {
    spec {
      containers {
        image = "${var.docker_image}:prod"
        env {
          name = "BUCKET_NAME"
          value = var.bucket_name
        }
        env {
          name = "DOCKER_IMAGE"
          value = "${var.docker_image}:prod"
        }
        env {
          name = "SERVICE_ACCOUNT"
          value = var.service_account
        }
        env {
          name = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "LOCATION"
          value = "europe-west9"
        }
      }
    } 
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run_api]
}

resource "google_cloud_run_service_iam_member" "run_all_users_prod" {
  service  = google_cloud_run_service.server_prod.name
  location = google_cloud_run_service.server_prod.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}



output "server_dev_url" {
  value = google_cloud_run_service.server_dev.status[0].url
}

output "server_prod_url" {
  value = google_cloud_run_service.server_prod.status[0].url
}
