variable "region" {
  type    = string
  default = "ap-south-1" # Mumbai
}

variable "project" {
  type    = string
  default = "airquality"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "monthly_budget_usd" {
  type    = string
  default = "5"
}

variable "alert_email" {
  type        = string
  description = "Email that receives the budget alert"
}
