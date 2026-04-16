# Criando o projeto no Terraform, precisa alterar a variável defalut para o ID do projeto que tem no GCP

variable "project" {
  description = "GCP project ID"
  type        = string
  default     = "project-bigdata-493514"
}

# Região da VM

variable "region" {
  description = "GCP region"
  type        = string
  default     = "southamerica-east1"
}

# Zona da VM

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "southamerica-east1-b"
}

# Tipo de máquina da VM

variable "machine_type" {
  description = "GCP machine type"
  type        = string
  default     = "n2-standard-8"
}

# Tamanho do disco da VM

variable "disk_size" {
  description = "Boot disk size in GB"
  type        = number
  default     = 60
}