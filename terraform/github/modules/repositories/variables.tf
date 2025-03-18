variable "name" {
  type        = string
  description = "The name of the repository."
}

variable "description" {
  type        = string
  description = "A short description of the repository."
  default     = ""
}

variable "visibility" {
  type        = string
  description = "The visibility of the repository (public or private)."
  validation {
    condition     = contains(["public", "private"], var.visibility)
    error_message = "Visibility must be either 'public' or 'private'."
  }
}
