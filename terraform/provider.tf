terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

# Criando o Provider que faz a comunicação com os recursos externos do terraform
provider "google" {
  credentials = file("key_gcp.json")
  project     = var.project
  region      = var.region
}