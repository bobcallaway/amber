resource "github_team" "this" {
  name        = var.name
  description = var.description
  privacy     = var.privacy
  # Add other team settings as needed
}
