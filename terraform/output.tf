output "server_dev_url" {
  value = google_cloud_run_service.server_dev.status[0].url
}

output "server_prod_url" {
  value = google_cloud_run_service.server_prod.status[0].url
}
