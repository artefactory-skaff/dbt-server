output "server_url" {
  value = google_cloud_run_service.server.status[0].url
}
