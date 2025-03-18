variable "name" {
  type        = string
  description = "The name of the team."
}

variable "description" {
  type        = string
  description = "A short description of the team."
  default     = ""
}

variable "privacy" {
  type        = string
  description = "The privacy of the team (secret or closed)."
  validation {
    condition     = contains(["secret", "closed"], var.privacy)
    error_message = "Privacy must be either 'secret' or 'closed'."
  }
}
