resource "google_service_account" "terraform-server-sa" {
  account_id   = "terraform-server-sa"
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
  role    = each.key
  member  = "serviceAccount:${google_service_account.terraform-server-sa.email}"
  project = var.project_id
}


resource "google_service_account" "terraform-job-sa" {
  account_id   = "terraform-job-sa"
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
  role    = each.key
  member  = "serviceAccount:${google_service_account.terraform-job-sa.email}"
  project = var.project_id
}