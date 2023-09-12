variable "server_name" {
  description = "The Cloud Run server name"
  type        = string
}

variable "bucket_name" {
  description = "The GCS bucket name"
  type        = string
}

variable "docker_image" {
  description = "The docker image"
  type        = string
}

variable "location" {
  description = "The location"
  type        = string
}

variable "project_id" {
  description = "The project ID"
  type        = string
}

variable "server_sa_email" {
  description = "The server service account email"
  type        = string
}

variable "job_sa_email" {
  description = "The job service account email"
  type        = string
}
