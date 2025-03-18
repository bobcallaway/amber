output "id" {
  description = "The ID of the team."
  value       = github_team.this.id
}

output "slug" {
  description = "The slug of the team."
  value       = github_team.this.slug
}
