module "repositories" {
  source = "./modules/repositories"
  # Add variables for the repositories module
}

module "teams" {
  source = "./modules/teams"
  # Add variables for the teams module
}

# module "users" {
#   source = "./modules/users"
#   # Add variables for the users module
# }

# module "bots" {
#   source = "./modules/bots"
#   # Add variables for the bots module
# }
