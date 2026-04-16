resource "google_compute_address" "static" {
  name = "static-ip"
}

# Criando uma instância de uma virtual machine

resource "google_compute_instance" "vm-terraform" {
  count        = 1
  name         = "vm-project-bigdata-${count.index}"
  machine_type = var.machine_type
  zone         = var.zone

# Configurando o boot de inicialização da maquina virtual, utilizando a imagem e o tamanho do disco
  boot_disk {
    initialize_params {
      image = "ubuntu-2204-jammy-v20240112"
      size  = var.disk_size
    }
  }

# criando a interface de comunicação da internet da VM e Configurando o ip como estático somente na primeira VM

  network_interface {
    network = "default"

    // Condição para atribuir o IP estático apenas à primeira máquina
    access_config {
      nat_ip = count.index == 0 ? google_compute_address.static.address : null
    }
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -xe

    # Instalando o Docker na VM
    curl -fsSL https://get.docker.com -o get-docker.sh
    cd ..
    cd ..
    sh get-docker.sh
    
  EOT
}
