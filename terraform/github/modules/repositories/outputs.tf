output "id" {
  description = "The ID of the repository."
  value       = github_repository.this.id
}

output "full_name" {
  description = "The full name of the repository (organization/name)."
  value       = github_repository.this.full_name
}
