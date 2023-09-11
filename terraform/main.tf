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

variable "docker_image" {
  type = string
  default = "us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image"
}

variable "bucket_name" {
  type = string
  default = "dbt-stc-test-eu"
}

variable "location" {
  type = string
  default = "europe-west9"
}


resource "google_service_account" "terraform-server-sa" {
  account_id = "terraform-server-sa"
  display_name = "terraform-server-sa"
}

resource "google_project_iam_member" "terraform-server-sa-permissions" {
  for_each = toset([
    "roles/datastore.owner",
    "roles/logging.logWriter",
    "roles/logging.viewer",
    "roles/storage.admin",
    "roles/run.developer",
    "roles/iam.serviceAccountUser"
  ])
  role = each.key
  member = "serviceAccount:${google_service_account.terraform-server-sa.email}"
  project = var.project_id
}


resource "google_service_account" "terraform-job-sa" {
  account_id = "terraform-job-sa"
  display_name = "terraform-job-sa"
}

resource "google_project_iam_member" "terraform-job-sa-permissions" {
  for_each = toset([
    "roles/datastore.user",
    "roles/logging.logWriter",
    "roles/logging.viewer",
    "roles/storage.admin",

    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigquery.dataViewer",
    "roles/bigquery.metadataViewer",
  ])
  role = each.key
  member = "serviceAccount:${google_service_account.terraform-job-sa.email}"
  project = var.project_id
}



resource "google_storage_bucket" "static" {
  name          = "dbt-stc-test-eu"
  location      = "EUROPE-WEST9"
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
  fields      = "{\"cloud_storage_folder\":{\"stringValue\":\"2023-09-08\"},\"log_starting_byte\":{\"stringValue\":\"0\"},\"run_status\":{\"stringValue\":\"started\"},\"user_command\":{\"stringValue\":\"\"},\"uuid\":{\"stringValue\":\"0\"}}"
}



resource "google_project_service" "run_api" {
  service = "run.googleapis.com"

  disable_on_destroy = true
}


# dbt-server dev on Cloud Run
resource "google_cloud_run_service" "server_dev" {
  name = "server-dev-tf"
  location = var.location

  template {
    spec {
      service_account_name = google_service_account.terraform-server-sa.email
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
          name = "SERVICE_ACCOUNT" // this service account is used by the dbt job
          value = google_service_account.terraform-job-sa.email
        }
        env {
          name = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "LOCATION"
          value = var.location
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run_api, google_project_iam_member.terraform-job-sa-permissions, google_project_iam_member.terraform-server-sa-permissions]
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
  location = var.location

  template {
    spec {
      service_account_name = google_service_account.terraform-server-sa.email
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
          value = google_service_account.terraform-job-sa.email
        }
        env {
          name = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "LOCATION"
          value = var.location
        }
      }
    } 
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.run_api, google_project_iam_member.terraform-job-sa-permissions, google_project_iam_member.terraform-server-sa-permissions]
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
