# GCS bucket
resource "google_storage_bucket" "static" {
  name          = var.bucket_name
  location      = "EUROPE-WEST9"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}

# Firestore
resource "google_project_service" "firestore" {
  project = var.project_id
  service = "firestore.googleapis.com"
}

resource "google_firestore_document" "first_status" {
  project     = var.project_id
  collection  = "dbt-status"
  document_id = "0"
  fields      = "{\"storage_folder\":{\"stringValue\":\"2023-09-08\"},\"log_starting_byte\":{\"stringValue\":\"0\"},\"run_status\":{\"stringValue\":\"started\"},\"user_command\":{\"stringValue\":\"\"},\"uuid\":{\"stringValue\":\"0\"}}"
}


# Enable Cloud Run API
resource "google_project_service" "run_api" {
  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

# dbt-server (dev) on Cloud Run
module "server_dev" {
  source = "./modules/dbt-server"

  server_name     = "server-dev-tf"
  bucket_name     = var.bucket_name
  docker_image    = "${var.docker_image}:dev"
  location        = var.location
  project_id      = var.project_id
  server_sa_email = google_service_account.terraform-server-sa.email
  job_sa_email    = google_service_account.terraform-job-sa.email

  depends_on = [google_project_service.run_api]
}

# dbt-server (prod) on Cloud Run
module "server_prod" {
  source = "./modules/dbt-server"

  server_name     = "server-prod-tf"
  bucket_name     = var.bucket_name
  docker_image    = "${var.docker_image}:prod"
  location        = var.location
  project_id      = var.project_id
  server_sa_email = google_service_account.terraform-server-sa.email
  job_sa_email    = google_service_account.terraform-job-sa.email

  depends_on = [google_project_service.run_api]
}
