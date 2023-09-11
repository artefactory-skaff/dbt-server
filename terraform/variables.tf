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
