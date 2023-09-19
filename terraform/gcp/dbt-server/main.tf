resource "google_cloud_run_service" "server" {
  name     = var.server_name
  location = var.location

  template {
    spec {
      service_account_name = var.server_sa_email
      containers {
        image = var.docker_image
        env {
          name  = "BUCKET_NAME"
          value = var.bucket_name
        }
        env {
          name  = "DOCKER_IMAGE"
          value = var.docker_image
        }
        env {
          name  = "SERVICE_ACCOUNT" // this service account is used by the dbt job
          value = var.job_sa_email
        }
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "LOCATION"
          value = var.location
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

}

resource "google_cloud_run_service_iam_member" "run_all_users" {
  service  = google_cloud_run_service.server.name
  location = google_cloud_run_service.server.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}
